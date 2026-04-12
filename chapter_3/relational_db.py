"""
Social network schema — Relational model (SQLite).

Tables: users, friendships, posts, comments, tags
Normalized design: each entity in its own table, joined via foreign keys.
Tags represent user mentions — each tag links a post to a tagged user.
"""

import sqlite3

DATABASE = "social_network.db"


def get_db_connection():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_db_connection()

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            username    TEXT    NOT NULL UNIQUE,
            name        TEXT    NOT NULL,
            email       TEXT    NOT NULL UNIQUE,
            bio         TEXT    DEFAULT '',
            created_at  TEXT    DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS friendships (
            user_id     INTEGER NOT NULL REFERENCES users(id),
            friend_id   INTEGER NOT NULL REFERENCES users(id),
            created_at  TEXT    DEFAULT (datetime('now')),
            PRIMARY KEY (user_id, friend_id),
            CHECK (user_id != friend_id)
        );

        CREATE TABLE IF NOT EXISTS posts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            author_id   INTEGER NOT NULL REFERENCES users(id),
            content     TEXT    NOT NULL,
            created_at  TEXT    DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS comments (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id     INTEGER NOT NULL REFERENCES posts(id),
            author_id   INTEGER NOT NULL REFERENCES users(id),
            content     TEXT    NOT NULL,
            created_at  TEXT    DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS tags (
            post_id     INTEGER NOT NULL REFERENCES posts(id),
            tagged_user_id INTEGER NOT NULL REFERENCES users(id),
            created_at  TEXT    DEFAULT (datetime('now')),
            PRIMARY KEY (post_id, tagged_user_id)
        );
    """)

    conn.commit()
    conn.close()


# ── CRUD helpers ──────────────────────────────────────────────

def create_user(username, name, email, bio=""):
    conn = get_db_connection()
    cursor = conn.execute(
        "INSERT INTO users (username, name, email, bio) VALUES (?, ?, ?, ?)",
        (username, name, email, bio),
    )
    conn.commit()
    uid = cursor.lastrowid
    conn.close()
    return uid


def add_friendship(user_id, friend_id):
    """Bidirectional friendship — inserts both directions."""
    conn = get_db_connection()
    conn.execute(
        "INSERT OR IGNORE INTO friendships (user_id, friend_id) VALUES (?, ?)",
        (user_id, friend_id),
    )
    conn.execute(
        "INSERT OR IGNORE INTO friendships (user_id, friend_id) VALUES (?, ?)",
        (friend_id, user_id),
    )
    conn.commit()
    conn.close()


def create_post(author_id, content):
    conn = get_db_connection()
    cursor = conn.execute(
        "INSERT INTO posts (author_id, content) VALUES (?, ?)",
        (author_id, content),
    )
    conn.commit()
    pid = cursor.lastrowid
    conn.close()
    return pid


def create_comment(post_id, author_id, content):
    conn = get_db_connection()
    cursor = conn.execute(
        "INSERT INTO comments (post_id, author_id, content) VALUES (?, ?, ?)",
        (post_id, author_id, content),
    )
    conn.commit()
    cid = cursor.lastrowid
    conn.close()
    return cid


def tag_user(post_id, tagged_user_id):
    conn = get_db_connection()
    conn.execute(
        "INSERT OR IGNORE INTO tags (post_id, tagged_user_id) VALUES (?, ?)",
        (post_id, tagged_user_id),
    )
    conn.commit()
    conn.close()


# ── Queries ───────────────────────────────────────────────────

def get_friends(user_id):
    conn = get_db_connection()
    rows = conn.execute("""
        SELECT u.id, u.username, u.name
        FROM friendships f
        JOIN users u ON u.id = f.friend_id
        WHERE f.user_id = ?
    """, (user_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_feed(user_id):
    """Posts from friends, ordered newest first."""
    conn = get_db_connection()
    rows = conn.execute("""
        SELECT p.id, p.content, p.created_at, u.username AS author
        FROM posts p
        JOIN friendships f ON f.friend_id = p.author_id
        JOIN users u ON u.id = p.author_id
        WHERE f.user_id = ?
        ORDER BY p.created_at DESC
    """, (user_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_posts_by_user(user_id):
    """Find all posts authored by a given user."""
    conn = get_db_connection()
    rows = conn.execute("""
        SELECT p.id, p.content, p.created_at, u.username AS author
        FROM posts p
        JOIN users u ON u.id = p.author_id
        WHERE p.author_id = ?
        ORDER BY p.created_at DESC
    """, (user_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_friends_of_friends(user_id):
    """2-hop friend discovery — people your friends know that you don't."""
    conn = get_db_connection()
    rows = conn.execute("""
        SELECT DISTINCT u.id, u.username, u.name
        FROM friendships f1
        JOIN friendships f2 ON f2.user_id = f1.friend_id
        JOIN users u ON u.id = f2.friend_id
        WHERE f1.user_id = ?
          AND f2.friend_id != ?
          AND f2.friend_id NOT IN (
              SELECT friend_id FROM friendships WHERE user_id = ?
          )
    """, (user_id, user_id, user_id)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_posts_tagging_user(user_id):
    """Find all posts where a user has been tagged."""
    conn = get_db_connection()
    rows = conn.execute("""
        SELECT p.id, p.content, p.created_at, u.username AS author
        FROM tags t
        JOIN posts p ON p.id = t.post_id
        JOIN users u ON u.id = p.author_id
        WHERE t.tagged_user_id = ?
        ORDER BY p.created_at DESC
    """, (user_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_posts_tagging_user_by_friends_of(tagged_user_id, user_id):
    """Posts that tag user Y, authored by friends of user X."""
    conn = get_db_connection()
    rows = conn.execute("""
        SELECT p.id, p.content, p.created_at, u.username AS author
        FROM tags t
        JOIN posts p ON p.id = t.post_id
        JOIN users u ON u.id = p.author_id
        JOIN friendships f ON f.friend_id = p.author_id
        WHERE t.tagged_user_id = ?
          AND f.user_id = ?
        ORDER BY p.created_at DESC
    """, (tagged_user_id, user_id)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_post_with_details(post_id):
    conn = get_db_connection()
    post = conn.execute("""
        SELECT p.*, u.username AS author
        FROM posts p JOIN users u ON u.id = p.author_id
        WHERE p.id = ?
    """, (post_id,)).fetchone()

    comments = conn.execute("""
        SELECT c.content, c.created_at, u.username AS author
        FROM comments c JOIN users u ON u.id = c.author_id
        WHERE c.post_id = ?
        ORDER BY c.created_at
    """, (post_id,)).fetchall()

    tagged_users = conn.execute("""
        SELECT u.id, u.username
        FROM tags t JOIN users u ON u.id = t.tagged_user_id
        WHERE t.post_id = ?
    """, (post_id,)).fetchall()

    conn.close()
    if post is None:
        return None
    return {
        **dict(post),
        "comments": [dict(c) for c in comments],
        "tagged_users": [dict(u) for u in tagged_users],
    }


# ── Demo ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import os
    if os.path.exists(DATABASE):
        os.remove(DATABASE)
    init_db()

    alice = create_user("alice", "Alice Smith", "alice@example.com", bio="Loves distributed systems")
    bob = create_user("bob", "Bob Jones", "bob@example.com", bio="Graph enthusiast")
    carol = create_user("carol", "Carol Lee", "carol@example.com", bio="Document model fan")
    dave = create_user("dave", "Dave Kim", "dave@example.com", bio="New here")

    add_friendship(alice, bob)
    add_friendship(alice, carol)
    add_friendship(bob, dave)

    p1 = create_post(bob, "Just finished DDIA chapter 3 with @alice and @carol!")
    p2 = create_post(carol, "Document databases are underrated.")
    create_comment(p1, alice, "Great chapter on storage engines!")
    create_comment(p1, carol, "LSM-trees are fascinating.")
    tag_user(p1, alice)
    tag_user(p1, carol)
    tag_user(p2, dave)

    print("=== Relational (SQLite) ===\n")
    print(f"Alice's friends: {get_friends(alice)}")
    print(f"\nAlice's feed: {get_feed(alice)}")
    print(f"\nAlice's friend suggestions: {get_friends_of_friends(alice)}")
    print(f"\nPost details: {get_post_with_details(p1)}")
    print(f"\nBob's posts: {get_posts_by_user(bob)}")
    print(f"\nPosts tagging Alice: {get_posts_tagging_user(alice)}")
    print(f"\nPosts tagging Carol by Alice's friends: {get_posts_tagging_user_by_friends_of(carol, alice)}")

    os.remove(DATABASE)
