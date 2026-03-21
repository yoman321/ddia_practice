import time
from datetime import datetime
from flask import Flask, request, jsonify, g
from storage import (
    get_client, init_storage,
    get_next_id, put_object, get_object, list_objects, delete_object,
    record_latency, RESERVATIONS_BUCKET,
)

app = Flask(__name__)
init_storage()


@app.before_request
def start_timer():
    g.start_time = time.perf_counter()


@app.after_request
def log_latency(response):
    duration_ms = (time.perf_counter() - g.start_time) * 1000
    record_latency(request.method, request.path, response.status_code, duration_ms)
    return response


@app.route("/reservations", methods=["GET"])
def get_reservations():
    client = get_client()
    reservations = list_objects(client, RESERVATIONS_BUCKET)
    return jsonify(reservations)


@app.route("/reservations/<reservation_id>", methods=["GET"])
def get_reservation(reservation_id):
    client = get_client()
    reservation = get_object(client, RESERVATIONS_BUCKET, reservation_id)
    if reservation is None:
        return jsonify({"error": "Reservation not found"}), 404
    return jsonify(reservation)


@app.route("/reservations", methods=["POST"])
def create_reservation():
    data = request.get_json()
    required_fields = ["guest_name", "date", "time", "party_size"]
    missing = [f for f in required_fields if f not in data]
    if missing:
        return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

    client = get_client()
    reservation_id = get_next_id(client, RESERVATIONS_BUCKET)
    reservation = {
        "id": reservation_id,
        "guest_name": data["guest_name"],
        "date": data["date"],
        "time": data["time"],
        "party_size": data["party_size"],
        "notes": data.get("notes", ""),
        "created_at": datetime.utcnow().isoformat(),
    }
    put_object(client, RESERVATIONS_BUCKET, str(reservation_id), reservation)
    return jsonify(reservation), 201


@app.route("/reservations/<int:reservation_id>", methods=["PUT"])
def update_reservation(reservation_id):
    client = get_client()
    existing = get_object(client, RESERVATIONS_BUCKET, str(reservation_id))
    if existing is None:
        return jsonify({"error": "Reservation not found"}), 404

    data = request.get_json()
    existing.update({k: data[k] for k in data if k in ["guest_name", "date", "time", "party_size", "notes"]})
    put_object(client, RESERVATIONS_BUCKET, str(reservation_id), existing)
    return jsonify(existing)


@app.route("/reservations/<int:reservation_id>", methods=["DELETE"])
def delete_reservation(reservation_id):
    client = get_client()
    existing = get_object(client, RESERVATIONS_BUCKET, str(reservation_id))
    if existing is None:
        return jsonify({"error": "Reservation not found"}), 404

    delete_object(client, RESERVATIONS_BUCKET, str(reservation_id))
    return jsonify({"message": f"Reservation {reservation_id} deleted successfully"})


if __name__ == "__main__":
    app.run(debug=True, port=5006)
