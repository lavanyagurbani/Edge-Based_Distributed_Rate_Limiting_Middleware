from threading import Thread
from workers.sync_worker import SyncWorker
import json
import os
import time
import logging
from flask import Flask, jsonify, request, send_from_directory
from services.redis_service import RedisService
from rate_limiter.factory import get_limiter
from services.event_publisher import publish_event
from config import Config

app = Flask(__name__)
@app.route('/dashboard', methods=['GET'])
def dashboard():
    return send_from_directory('static', 'dashboard.html')
# Helper: validate config payload based on algorithm
def _validate_config(payload):
    alg = payload.get('algorithm')
    if alg not in ('token_bucket', 'sliding_window'):
        return False, 'Unsupported algorithm'
    base_fields = {'algorithm', 'limit'}
    if alg == 'token_bucket':
        required = base_fields.union({'bucket_capacity', 'refill_rate'})
    else:  # sliding_window
        required = base_fields.union({'window_seconds'})
    missing = required - payload.keys()
    if missing:
        return False, f"Missing fields: {', '.join(sorted(missing))}"
    return True, ''

# Simple health endpoints
@app.route('/', methods=['GET'])
def index():
    return jsonify({"message": "Edge-Based Distributed Rate Limiting Middleware"})

@app.route('/health/live', methods=['GET'])
def health_live():
    return jsonify({"status": "live"})

@app.route('/health/ready', methods=['GET'])
def health_ready():
    ready = RedisService().ping()
    status = "ready" if ready else "unready"
    return jsonify({"status": status, "redis": ready})

# Configuration endpoint – replace active policy
@app.route('/api/config', methods=['POST'])
def set_config():
    data = request.get_json(force=True)
    ok, msg = _validate_config(data)
    if not ok:
        return jsonify({"error": msg}), 400
    redis_srv = RedisService()
    policy_id = redis_srv.store_policy(data)
    # Update active_policy_id atomically (SET is sufficient for our test environment)
    redis_srv.client.set('active_policy_id', policy_id)
    return jsonify({"policy_id": policy_id}), 200

# Request decision endpoint
@app.route('/api/request', methods=['POST'])
def request_decision():
    payload = request.get_json(force=True)
    for field in ("user_id", "edge_id", "request_id"):
        if field not in payload:
            return jsonify({"error": f"Missing {field}"}), 400
    user_id = payload["user_id"]
    edge_id = payload["edge_id"]
    request_id = payload["request_id"]

    redis_srv = RedisService()
    policy_id = redis_srv.get_active_policy_id()
    policy = redis_srv.get_policy(policy_id)
    limiter = get_limiter(policy["algorithm"])
    result = limiter.is_allowed(user_id, edge_id, policy_id, request_id)
    # Record request decision in per-policy history
    redis_srv.add_history(policy_id, {
        "user_id": user_id,
        "edge_id": edge_id,
        "request_id": request_id,
        "allowed": result.allowed,
        "retry_after": result.retry_after,
        "remaining": result.remaining,
        "timestamp": int(time.time() * 1000),
    })
    # Publish async event for synchronization worker
    try:
        publish_event({
            "user_id": user_id,
            "edge_id": edge_id,
            "request_id": request_id,
            "policy_id": policy_id,
            "allowed": result.allowed,
            "timestamp": int(time.time() * 1000),
        })
    except Exception as exc:
        logging.getLogger(__name__).debug("Publish event failed: %s", exc)
    response = {
        "allowed": result.allowed,
        "retry_after": result.retry_after,
        "remaining": result.remaining,
        "policy_id": policy_id,
    }
    status_code = 200 if result.allowed else 429
    return jsonify(response), status_code

# Simulate endpoint – mirrors /api/request
@app.route('/api/simulate', methods=['POST'])
def simulate():
    return request_decision()

# Metrics endpoint – returns simple counters if present
@app.route('/api/metrics', methods=['GET'])
def metrics():
    client = RedisService().client
    keys = ["metrics:total", "metrics:allowed", "metrics:rejected"]
    data = {k.split(':')[1]: int(client.get(k) or 0) for k in keys}
    return jsonify({"metrics": data})

# History endpoint – placeholder (returns empty list)
@app.route('/api/history', methods=['GET'])
def history():
    redis_srv = RedisService()
    policy_id = request.args.get('policy_id') or redis_srv.get_active_policy_id()
    hist = redis_srv.get_history(policy_id)
    return jsonify({"policy_id": policy_id, "history": hist})

# Sync status endpoint – placeholder (RabbitMQ/worker health)
@app.route('/api/sync/status', methods=['GET'])
def sync_status():
    # RabbitMQ health – attempt a connection check
    rabbitmq_status = "unavailable"
    try:
        import pika
        params = pika.URLParameters(Config.RABBITMQ_URL)
        connection = pika.BlockingConnection(params)
        if connection.is_open:
            rabbitmq_status = "available"
        connection.close()
    except Exception:
        rabbitmq_status = "unavailable"
    worker_status = RedisService().get_worker_status()
    return jsonify({"sync": {"rabbitmq": rabbitmq_status, "worker": worker_status}})

# Admin simulate failure – development only
@app.route('/api/admin/simulate-failure', methods=['POST'])
def simulate_failure():
    if os.getenv('FLASK_ENV') != 'development':
        return jsonify({"error": "Endpoint only available in development mode"}), 403
    app.config['SIMULATE_FAILURE'] = not app.config.get('SIMULATE_FAILURE', False)
    return jsonify({"simulate_failure": app.config['SIMULATE_FAILURE']})


if __name__ == '__main__':
    host = os.getenv('APP_HOST', '0.0.0.0')
    port = int(os.getenv('PORT', os.getenv('APP_PORT', 5000)))

    worker = SyncWorker()
    worker_thread = Thread(target=worker.run, daemon=True)
    worker_thread.start()

    app.run(
        host=host,
        port=port,
        debug=os.getenv('FLASK_DEBUG', '0') == '1',
        use_reloader=False
    )
