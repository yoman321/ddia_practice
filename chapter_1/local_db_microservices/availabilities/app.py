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


@app.route("/availabilities", methods=["GET"])
def get_availabilities():
    date = request.args.get("date")
    time_slot = request.args.get("time_slot")
    conn = get_db_connection()
    if date and time_slot:
        rows = conn.execute(
            "SELECT * FROM availabilities WHERE date = ? AND time_slot = ?",
            (date, time_slot)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM availabilities").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/availabilities/<int:availability_id>", methods=["GET"])
def get_availability(availability_id):
    conn = get_db_connection()
    row = conn.execute(
        "SELECT * FROM availabilities WHERE id = ?", (availability_id,)
    ).fetchone()
    conn.close()
    if row is None:
        return jsonify({"error": "Availability not found"}), 404
    return jsonify(dict(row))


@app.route("/availabilities", methods=["POST"])
def create_availability():
    data = request.get_json()
    required_fields = ["date", "time_slot", "total_capacity"]
    missing = [f for f in required_fields if f not in data]
    if missing:
        return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

    total_capacity = data["total_capacity"]
    conn = get_db_connection()
    cursor = conn.execute(
        """
        INSERT INTO availabilities (date, time_slot, total_capacity, booked, available)
        VALUES (?, ?, ?, 0, ?)
        """,
        (data["date"], data["time_slot"], total_capacity, total_capacity),
    )
    conn.commit()
    conn.close()
    return jsonify({
        "id": cursor.lastrowid,
        "date": data["date"],
        "time_slot": data["time_slot"],
        "total_capacity": total_capacity,
        "booked": 0,
        "available": total_capacity,
    }), 201


@app.route("/availabilities/<int:availability_id>", methods=["PUT"])
def update_availability(availability_id):
    conn = get_db_connection()
    existing = conn.execute(
        "SELECT * FROM availabilities WHERE id = ?", (availability_id,)
    ).fetchone()
    if existing is None:
        conn.close()
        return jsonify({"error": "Availability not found"}), 404

    data = request.get_json()
    updated = {
        "date": data.get("date", existing["date"]),
        "time_slot": data.get("time_slot", existing["time_slot"]),
        "total_capacity": data.get("total_capacity", existing["total_capacity"]),
        "booked": data.get("booked", existing["booked"]),
    }
    updated["available"] = updated["total_capacity"] - updated["booked"]

    conn.execute(
        """
        UPDATE availabilities
        SET date = ?, time_slot = ?, total_capacity = ?, booked = ?, available = ?
        WHERE id = ?
        """,
        (
            updated["date"], updated["time_slot"], updated["total_capacity"],
            updated["booked"], updated["available"], availability_id,
        ),
    )
    conn.commit()
    conn.close()
    return jsonify({**updated, "id": availability_id})


@app.route("/availabilities/<int:availability_id>/book", methods=["POST"])
def book_availability(availability_id):
    conn = get_db_connection()
    row = conn.execute(
        "SELECT * FROM availabilities WHERE id = ?", (availability_id,)
    ).fetchone()
    if row is None:
        conn.close()
        return jsonify({"error": "Availability not found"}), 404
    if row["available"] <= 0:
        conn.close()
        return jsonify({"error": "No availability left for this time slot"}), 409

    conn.execute(
        "UPDATE availabilities SET booked = booked + 1, available = available - 1 WHERE id = ?",
        (availability_id,)
    )
    conn.commit()
    row = conn.execute("SELECT * FROM availabilities WHERE id = ?", (availability_id,)).fetchone()
    conn.close()
    return jsonify(dict(row))


@app.route("/availabilities/<int:availability_id>/release", methods=["POST"])
def release_availability(availability_id):
    conn = get_db_connection()
    row = conn.execute(
        "SELECT * FROM availabilities WHERE id = ?", (availability_id,)
    ).fetchone()
    if row is None:
        conn.close()
        return jsonify({"error": "Availability not found"}), 404
    if row["booked"] <= 0:
        conn.close()
        return jsonify({"error": "Nothing to release for this time slot"}), 409

    conn.execute(
        "UPDATE availabilities SET booked = booked - 1, available = available + 1 WHERE id = ?",
        (availability_id,)
    )
    conn.commit()
    row = conn.execute("SELECT * FROM availabilities WHERE id = ?", (availability_id,)).fetchone()
    conn.close()
    return jsonify(dict(row))


@app.route("/availabilities/<int:availability_id>", methods=["DELETE"])
def delete_availability(availability_id):
    conn = get_db_connection()
    existing = conn.execute(
        "SELECT * FROM availabilities WHERE id = ?", (availability_id,)
    ).fetchone()
    if existing is None:
        conn.close()
        return jsonify({"error": "Availability not found"}), 404

    conn.execute("DELETE FROM availabilities WHERE id = ?", (availability_id,))
    conn.commit()
    conn.close()
    return jsonify({"message": f"Availability {availability_id} deleted successfully"})


if __name__ == "__main__":
    app.run(debug=True, port=5007)
