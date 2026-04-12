"""
Benchmark the three common queries across three server-backed backends:
  - Relational (SQLite via Flask server)
  - Document (MongoDB)
  - Graph (Neo4j)

All three go through a network layer for fair comparison.
1000 users, 10000 posts. Runs each query 10 times, averages, and charts.
"""

import os
import random
import time

import matplotlib.pyplot as plt
import numpy as np

import relational_db
import mongo_db
import neo4j_db
import sqlite_client

NUM_USERS = 1000
NUM_POSTS = 10000
NUM_FRIENDSHIPS_PER_USER = 10
NUM_TAGS_PER_POST = 2
NUM_RUNS = 10

random.seed(42)


# ── Seed helpers ──────────────────────────────────────────────

def seed_relational():
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
    return user_ids


def seed_document_mem():
    document_db._collections["users"] = {}
    document_db._next_id = 1

    user_ids = []
    for i in range(NUM_USERS):
        uid = document_db.create_user(f"user_{i}", f"User {i}", f"user_{i}@example.com", bio=f"Bio {i}")
        user_ids.append(uid)

    for uid in user_ids:
        friends = random.sample(user_ids, min(NUM_FRIENDSHIPS_PER_USER, len(user_ids)))
        for fid in friends:
            if fid != uid:
                document_db.add_friendship(uid, fid)

    post_map = []
    for i in range(NUM_POSTS):
        author = random.choice(user_ids)
        pid = document_db.create_post(author, f"Post content {i}")
        post_map.append((author, pid))

    for author, pid in post_map:
        tagged = random.sample(user_ids, min(NUM_TAGS_PER_POST, len(user_ids)))
        for tid in tagged:
            document_db.tag_user(author, pid, tid)

    return user_ids


def seed_graph_mem():
    g = graph_db.GraphDB()

    user_ids = []
    for i in range(NUM_USERS):
        uid = g.create_user(f"user_{i}", f"User {i}", f"user_{i}@example.com", bio=f"Bio {i}")
        user_ids.append(uid)

    for uid in user_ids:
        friends = random.sample(user_ids, min(NUM_FRIENDSHIPS_PER_USER, len(user_ids)))
        for fid in friends:
            if fid != uid:
                g.add_friendship(uid, fid)

    post_ids = []
    for i in range(NUM_POSTS):
        author = random.choice(user_ids)
        pid = g.create_post(author, f"Post content {i}")
        post_ids.append(pid)

    for pid in post_ids:
        tagged = random.sample(user_ids, min(NUM_TAGS_PER_POST, len(user_ids)))
        for tid in tagged:
            g.tag_user(pid, tid)

    return g, user_ids


def seed_mongo():
    mongo_db.drop_db()
    mongo_db.init_db()

    db = mongo_db.get_db()
    user_ids = []
    user_docs = []
    for i in range(NUM_USERS):
        user_docs.append({
            "username": f"user_{i}",
            "name": f"User {i}",
            "email": f"user_{i}@example.com",
            "bio": f"Bio {i}",
            "friends": [],
        })
    result = db.users.insert_many(user_docs)
    user_ids = list(result.inserted_ids)

    # Friendships (bulk updates)
    for idx, uid in enumerate(user_ids):
        friends = random.sample(user_ids, min(NUM_FRIENDSHIPS_PER_USER, len(user_ids)))
        friends = [fid for fid in friends if fid != uid]
        db.users.update_one({"_id": uid}, {"$addToSet": {"friends": {"$each": friends}}})
        for fid in friends:
            db.users.update_one({"_id": fid}, {"$addToSet": {"friends": uid}})

    # Posts
    post_docs = []
    post_authors = []
    for i in range(NUM_POSTS):
        author = random.choice(user_ids)
        post_authors.append(author)
        post_docs.append({
            "author_id": author,
            "content": f"Post content {i}",
            "tagged_user_ids": [],
            "comments": [],
        })
    result = db.posts.insert_many(post_docs)
    post_ids = list(result.inserted_ids)

    # Tags (bulk)
    for pid in post_ids:
        tagged = random.sample(user_ids, min(NUM_TAGS_PER_POST, len(user_ids)))
        db.posts.update_one({"_id": pid}, {"$addToSet": {"tagged_user_ids": {"$each": tagged}}})

    return user_ids


def seed_neo4j():
    neo4j_db.drop_db()
    neo4j_db.init_db()

    driver = neo4j_db.get_driver()

    user_ids = list(range(NUM_USERS))

    # Batch create users
    with driver.session() as s:
        s.run(
            "UNWIND $users AS u CREATE (:User {uid: u.uid, username: u.username, name: u.name, email: u.email, bio: u.bio})",
            users=[{"uid": i, "username": f"user_{i}", "name": f"User {i}",
                    "email": f"user_{i}@example.com", "bio": f"Bio {i}"} for i in user_ids],
        )

    # Batch friendships
    friendship_pairs = set()
    for uid in user_ids:
        friends = random.sample(user_ids, min(NUM_FRIENDSHIPS_PER_USER, len(user_ids)))
        for fid in friends:
            if fid != uid:
                pair = (min(uid, fid), max(uid, fid))
                friendship_pairs.add(pair)

    with driver.session() as s:
        s.run("""
            UNWIND $pairs AS p
            MATCH (a:User {uid: p[0]}), (b:User {uid: p[1]})
            CREATE (a)-[:FRIENDS_WITH]->(b)
            CREATE (b)-[:FRIENDS_WITH]->(a)
        """, pairs=[list(p) for p in friendship_pairs])

    # Batch posts
    post_data = []
    for i in range(NUM_POSTS):
        author = random.choice(user_ids)
        post_data.append({"pid": i, "author_uid": author, "content": f"Post content {i}"})

    with driver.session() as s:
        s.run("""
            UNWIND $posts AS p
            MATCH (u:User {uid: p.author_uid})
            CREATE (post:Post {pid: p.pid, content: p.content})
            CREATE (u)-[:AUTHORED]->(post)
        """, posts=post_data)

    # Batch tags
    tag_data = []
    for i in range(NUM_POSTS):
        tagged = random.sample(user_ids, min(NUM_TAGS_PER_POST, len(user_ids)))
        for tid in tagged:
            tag_data.append({"pid": i, "tagged_uid": tid})

    with driver.session() as s:
        s.run("""
            UNWIND $tags AS t
            MATCH (p:Post {pid: t.pid}), (u:User {uid: t.tagged_uid})
            CREATE (p)-[:TAGGED]->(u)
        """, tags=tag_data)

    return user_ids


# ── Benchmark runner ──────────────────────────────────────────

def bench(fn, runs=NUM_RUNS):
    times = []
    for _ in range(runs):
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)
    return sum(times) / len(times)


def run_benchmarks():
    print("Seeding Relational (SQLite server)...")
    rel_users = sqlite_client.seed(NUM_USERS, NUM_POSTS)
    print("Seeding Document (MongoDB)...")
    mongo_users = seed_mongo()
    print("Seeding Graph (Neo4j)...")
    neo4j_users = seed_neo4j()
    print(f"Done. {NUM_USERS} users, {NUM_POSTS} posts per backend.\n")

    # Test user indices
    test_idx = 0
    tagged_idx = 1

    # Build per-backend config: (label, fof_fn, posts_fn, tag_fn)
    backends = [
        (
            "Relational\n(SQLite server)",
            lambda: sqlite_client.get_friends_of_friends(rel_users[test_idx]),
            lambda: sqlite_client.get_posts_by_user(rel_users[test_idx]),
            lambda: sqlite_client.get_posts_tagging_user_by_friends_of(rel_users[tagged_idx], rel_users[test_idx]),
        ),
        (
            "Document\n(MongoDB)",
            lambda: mongo_db.get_friends_of_friends(mongo_users[test_idx]),
            lambda: mongo_db.get_posts_by_user(mongo_users[test_idx]),
            lambda: mongo_db.get_posts_tagging_user_by_friends_of(mongo_users[tagged_idx], mongo_users[test_idx]),
        ),
        (
            "Graph\n(Neo4j)",
            lambda: neo4j_db.get_friends_of_friends(neo4j_users[test_idx]),
            lambda: neo4j_db.get_posts_by_user(neo4j_users[test_idx]),
            lambda: neo4j_db.get_posts_tagging_user_by_friends_of(neo4j_users[tagged_idx], neo4j_users[test_idx]),
        ),
    ]

    queries = ["friends_of_friends", "posts_by_user", "posts_tagging_user_by_friends_of"]
    results = {b[0]: [] for b in backends}

    for qi, qname in enumerate(queries):
        print(f"Benchmarking {qname}...")
        for label, fof, pbu, ptubfo in backends:
            fn = [fof, pbu, ptubfo][qi]
            results[label].append(bench(fn))

    # Print table
    header_labels = [b[0].replace("\n", " ") for b in backends]
    print(f"\n{'Query':<35}", end="")
    for h in header_labels:
        print(f" {h:>18}", end="")
    print()
    print("-" * (35 + 19 * len(backends)))
    for i, q in enumerate(queries):
        print(f"{q:<35}", end="")
        for b in backends:
            val = results[b[0]][i] * 1000
            print(f" {val:>15.3f} ms", end="")
        print()

    # ── Chart ─────────────────────────────────────────────────
    x = np.arange(len(queries))
    n = len(backends)
    width = 0.15

    fig, ax = plt.subplots(figsize=(14, 7))

    colors = ["#4C72B0", "#DD8452", "#C44E52", "#55A868", "#8172B3"]

    for i, (label, *_) in enumerate(backends):
        times_ms = [results[label][j] * 1000 for j in range(len(queries))]
        bars = ax.bar(x + i * width, times_ms, width, label=label.replace("\n", " "), color=colors[i])
        for bar, val in zip(bars, times_ms):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                    f"{val:.2f}", ha="center", va="bottom", fontsize=7)

    ax.set_xlabel("Query")
    ax.set_ylabel("Avg time (ms)")
    ax.set_title(f"Query Performance: {NUM_USERS} users, {NUM_POSTS} posts (avg of {NUM_RUNS} runs)")
    ax.set_xticks(x + width * (n - 1) / 2)
    ax.set_xticklabels(queries, fontsize=9)
    ax.legend(fontsize=8)
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig("benchmark_results.png", dpi=150)
    print("\nChart saved to benchmark_results.png")
    plt.show()

    # Cleanup
    if os.path.exists(relational_db.DATABASE):
        os.remove(relational_db.DATABASE)
    mongo_db.drop_db()
    neo4j_db.drop_db()
    neo4j_db.close()


if __name__ == "__main__":
    run_benchmarks()
