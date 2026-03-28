import time
import requests as http_requests
from flask import Flask, request, jsonify, g
from database import init_db, get_db_connection

app = Flask(__name__)

init_db()

AVAILABILITIES_SERVICE = "http://127.0.0.1:5007"
OPEN_HOUR = 12   # 12pm
CLOSE_HOUR = 24  # 12am (midnight)


def validate_time(time_str):
    """Returns True if time is within restaurant hours (12:00 - 23:59)."""
    try:
        hour, minute = map(int, time_str.split(":"))
        return OPEN_HOUR <= hour < CLOSE_HOUR
    except Exception:
        return False


def find_availability(date, time_slot):
    """Look up availability slot by date and time. Returns slot dict or None."""
    resp = http_requests.get(
        f"{AVAILABILITIES_SERVICE}/availabilities",
        params={"date": date, "time_slot": time_slot}
    )
    slots = resp.json()
    return slots[0] if slots else None


def book_slot(availability_id):
    resp = http_requests.post(f"{AVAILABILITIES_SERVICE}/availabilities/{availability_id}/book")
    return resp.status_code, resp.json()


def release_slot(availability_id):
    http_requests.post(f"{AVAILABILITIES_SERVICE}/availabilities/{availability_id}/release")


@app.before_request
def start_timer():
    g.start_time = time.perf_counter()


@app.after_request
def record_latency(response):
    duration_ms = (time.perf_counter() - g.start_time) * 1000
    conn = get_db_connection()
    conn.execute(
        "INSERT INTO latency (method, endpoint, status_code, duration_ms) VALUES (?, ?, ?, ?)",
        (request.method, request.path, response.status_code, round(duration_ms, 3)),
    )
    conn.commit()
    conn.close()
    return response


@app.route("/reservations", methods=["GET"])
def get_reservations():
    conn = get_db_connection()
    reservations = conn.execute("SELECT * FROM reservations").fetchall()
    conn.close()
    return jsonify([dict(r) for r in reservations])


@app.route("/reservations/<int:reservation_id>", methods=["GET"])
def get_reservation(reservation_id):
    conn = get_db_connection()
    reservation = conn.execute(
        "SELECT * FROM reservations WHERE id = ?", (reservation_id,)
    ).fetchone()
    conn.close()
    if reservation is None:
        return jsonify({"error": "Reservation not found"}), 404
    return jsonify(dict(reservation))


@app.route("/reservations", methods=["POST"])
def create_reservation():
    data = request.get_json()
    required_fields = ["guest_name", "date", "time", "party_size"]
    missing = [f for f in required_fields if f not in data]
    if missing:
        return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

    if not validate_time(data["time"]):
        return jsonify({"error": f"Reservations only accepted between 12:00 and 23:59"}), 400

    slot = find_availability(data["date"], data["time"])
    if slot is None:
        return jsonify({"error": f"No availability configured for {data['date']} at {data['time']}"}), 409

    status, result = book_slot(slot["id"])
    if status != 200:
        return jsonify({"error": result.get("error", "Could not book slot")}), 409

    conn = get_db_connection()
    cursor = conn.execute(
        "INSERT INTO reservations (guest_name, date, time, party_size, notes) VALUES (?, ?, ?, ?, ?)",
        (data["guest_name"], data["date"], data["time"], data["party_size"], data.get("notes", "")),
    )
    conn.commit()
    conn.close()
    return jsonify({
        "id": cursor.lastrowid,
        "guest_name": data["guest_name"],
        "date": data["date"],
        "time": data["time"],
        "party_size": data["party_size"],
        "notes": data.get("notes", ""),
    }), 201


@app.route("/reservations/<int:reservation_id>", methods=["PUT"])
def update_reservation(reservation_id):
    conn = get_db_connection()
    existing = conn.execute(
        "SELECT * FROM reservations WHERE id = ?", (reservation_id,)
    ).fetchone()
    if existing is None:
        conn.close()
        return jsonify({"error": "Reservation not found"}), 404

    data = request.get_json()
    updated = {
        "guest_name": data.get("guest_name", existing["guest_name"]),
        "date": data.get("date", existing["date"]),
        "time": data.get("time", existing["time"]),
        "party_size": data.get("party_size", existing["party_size"]),
        "notes": data.get("notes", existing["notes"]),
    }

    time_or_date_changed = (
        updated["date"] != existing["date"] or updated["time"] != existing["time"]
    )

    if time_or_date_changed:
        if not validate_time(updated["time"]):
            conn.close()
            return jsonify({"error": "Reservations only accepted between 12:00 and 23:59"}), 400

        new_slot = find_availability(updated["date"], updated["time"])
        if new_slot is None:
            conn.close()
            return jsonify({"error": f"No availability configured for {updated['date']} at {updated['time']}"}), 409

        status, result = book_slot(new_slot["id"])
        if status != 200:
            conn.close()
            return jsonify({"error": result.get("error", "Could not book new slot")}), 409

        old_slot = find_availability(existing["date"], existing["time"])
        if old_slot:
            release_slot(old_slot["id"])

    conn.execute(
        "UPDATE reservations SET guest_name = ?, date = ?, time = ?, party_size = ?, notes = ? WHERE id = ?",
        (updated["guest_name"], updated["date"], updated["time"], updated["party_size"], updated["notes"], reservation_id),
    )
    conn.commit()
    conn.close()
    return jsonify({"id": reservation_id, **updated})


@app.route("/reservations/<int:reservation_id>", methods=["DELETE"])
def delete_reservation(reservation_id):
    conn = get_db_connection()
    existing = conn.execute(
        "SELECT * FROM reservations WHERE id = ?", (reservation_id,)
    ).fetchone()
    if existing is None:
        conn.close()
        return jsonify({"error": "Reservation not found"}), 404

    slot = find_availability(existing["date"], existing["time"])
    if slot:
        release_slot(slot["id"])

    conn.execute("DELETE FROM reservations WHERE id = ?", (reservation_id,))
    conn.commit()
    conn.close()
    return jsonify({"message": f"Reservation {reservation_id} deleted successfully"})


if __name__ == "__main__":
    app.run(debug=True, port=5005)
