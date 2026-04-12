"""
SQLite wrapped in a Flask server to simulate client-server architecture.
Exposes the same three queries as HTTP endpoints.
"""

import os
from flask import Flask, request, jsonify
import relational_db

app = Flask(__name__)


@app.route("/friends_of_friends", methods=["GET"])
def friends_of_friends():
    user_id = int(request.args["user_id"])
    return jsonify(relational_db.get_friends_of_friends(user_id))


@app.route("/posts_by_user", methods=["GET"])
def posts_by_user():
    user_id = int(request.args["user_id"])
    return jsonify(relational_db.get_posts_by_user(user_id))


@app.route("/posts_tagging_user_by_friends_of", methods=["GET"])
def posts_tagging_user_by_friends_of():
    tagged_user_id = int(request.args["tagged_user_id"])
    user_id = int(request.args["user_id"])
    return jsonify(relational_db.get_posts_tagging_user_by_friends_of(tagged_user_id, user_id))


@app.route("/seed", methods=["POST"])
def seed():
    """Seed the database — called by the benchmark before running queries."""
    import random
    random.seed(42)

    NUM_USERS = int(request.args.get("num_users", 1000))
    NUM_POSTS = int(request.args.get("num_posts", 10000))
    NUM_FRIENDSHIPS_PER_USER = 10
    NUM_TAGS_PER_POST = 2

    if os.path.exists(relational_db.DATABASE):
        os.remove(relational_db.DATABASE)
    relational_db.init_db()

    conn = relational_db.get_db_connection()
    user_ids = []
    for i in range(NUM_USERS):
        cursor = conn.execute(
            "INSERT INTO users (username, name, email, bio) VALUES (?, ?, ?, ?)",
            (f"user_{i}", f"User {i}", f"user_{i}@example.com", f"Bio {i}"),
        )
        user_ids.append(cursor.lastrowid)
    conn.commit()

    for uid in user_ids:
        friends = random.sample(user_ids, min(NUM_FRIENDSHIPS_PER_USER, len(user_ids)))
        for fid in friends:
            if fid != uid:
                conn.execute("INSERT OR IGNORE INTO friendships (user_id, friend_id) VALUES (?, ?)", (uid, fid))
                conn.execute("INSERT OR IGNORE INTO friendships (user_id, friend_id) VALUES (?, ?)", (fid, uid))
    conn.commit()

    post_ids = []
    for i in range(NUM_POSTS):
        author = random.choice(user_ids)
        cursor = conn.execute("INSERT INTO posts (author_id, content) VALUES (?, ?)", (author, f"Post content {i}"))
        post_ids.append(cursor.lastrowid)
    conn.commit()

    for pid in post_ids:
        tagged = random.sample(user_ids, min(NUM_TAGS_PER_POST, len(user_ids)))
        for tid in tagged:
            conn.execute("INSERT OR IGNORE INTO tags (post_id, tagged_user_id) VALUES (?, ?)", (pid, tid))
    conn.commit()
    conn.close()

    return jsonify({"user_ids": user_ids})


if __name__ == "__main__":
    app.run(port=5020, debug=False)
