import time
from flask import Flask, request, jsonify, g
from database import init_db, get_db_connection

app = Flask(__name__)

init_db()


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

    conn = get_db_connection()
    cursor = conn.execute(
        """
        INSERT INTO reservations (guest_name, date, time, party_size, notes)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            data["guest_name"],
            data["date"],
            data["time"],
            data["party_size"],
            data.get("notes", ""),
        ),
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

    conn.execute(
        """
        UPDATE reservations
        SET guest_name = ?, date = ?, time = ?, party_size = ?, notes = ?
        WHERE id = ?
        """,
        (
            updated["guest_name"],
            updated["date"],
            updated["time"],
            updated["party_size"],
            updated["notes"],
            reservation_id,
        ),
    )
    conn.commit()
    conn.close()
    return jsonify({
        "id": reservation_id,
        "guest_name": updated["guest_name"],
        "date": updated["date"],
        "time": updated["time"],
        "party_size": updated["party_size"],
        "notes": updated["notes"],
    })


@app.route("/reservations/<int:reservation_id>", methods=["DELETE"])
def delete_reservation(reservation_id):
    conn = get_db_connection()
    existing = conn.execute(
        "SELECT * FROM reservations WHERE id = ?", (reservation_id,)
    ).fetchone()
    if existing is None:
        conn.close()
        return jsonify({"error": "Reservation not found"}), 404

    conn.execute("DELETE FROM reservations WHERE id = ?", (reservation_id,))
    conn.commit()
    conn.close()
    return jsonify({"message": f"Reservation {reservation_id} deleted successfully"})


if __name__ == "__main__":
    app.run(debug=True, port=5005)
