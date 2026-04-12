"""
Social network schema — Document model (MongoDB).

Collections: users, posts
Tags are stored as arrays of user IDs inside each post document.
Same queries as the in-memory document_db, but backed by real MongoDB.
"""

from pymongo import MongoClient

DATABASE_NAME = "social_network"

_client = None
_db = None


def get_db():
    global _client, _db
    if _db is None:
        _client = MongoClient("mongodb://localhost:27017")
        _db = _client[DATABASE_NAME]
    return _db


def init_db():
    db = get_db()
    db.users.create_index("username", unique=True)
    db.posts.create_index("author_id")
    db.posts.create_index("tagged_user_ids")


def drop_db():
    db = get_db()
    _client.drop_database(DATABASE_NAME)


# ── CRUD helpers ──────────────────────────────────────────────

def create_user(username, name, email, bio=""):
    db = get_db()
    result = db.users.insert_one({
        "username": username,
        "name": name,
        "email": email,
        "bio": bio,
        "friends": [],
    })
    return result.inserted_id


def add_friendship(user_id, friend_id):
    db = get_db()
    db.users.update_one({"_id": user_id}, {"$addToSet": {"friends": friend_id}})
    db.users.update_one({"_id": friend_id}, {"$addToSet": {"friends": user_id}})


def create_post(author_id, content):
    db = get_db()
    result = db.posts.insert_one({
        "author_id": author_id,
        "content": content,
        "tagged_user_ids": [],
        "comments": [],
    })
    return result.inserted_id


def tag_user(post_id, tagged_user_id):
    db = get_db()
    db.posts.update_one({"_id": post_id}, {"$addToSet": {"tagged_user_ids": tagged_user_id}})


def create_comment(post_id, author_id, content):
    db = get_db()
    db.posts.update_one({"_id": post_id}, {"$push": {"comments": {
        "author_id": author_id,
        "content": content,
    }}})


# ── Queries ───────────────────────────────────────────────────

def get_friends_of_friends(user_id):
    db = get_db()
    user = db.users.find_one({"_id": user_id})
    direct = set(user["friends"])
    suggestions = {}
    for fid in user["friends"]:
        friend = db.users.find_one({"_id": fid})
        for fof_id in friend["friends"]:
            if fof_id != user_id and fof_id not in direct:
                fof = db.users.find_one({"_id": fof_id})
                suggestions[fof_id] = {
                    "_id": fof_id,
                    "username": fof["username"],
                    "name": fof["name"],
                }
    return list(suggestions.values())


def get_posts_by_user(user_id):
    db = get_db()
    user = db.users.find_one({"_id": user_id})
    posts = list(db.posts.find({"author_id": user_id}))
    return [
        {"_id": p["_id"], "content": p["content"], "author": user["username"]}
        for p in posts
    ]


def get_posts_tagging_user_by_friends_of(tagged_user_id, user_id):
    db = get_db()
    user = db.users.find_one({"_id": user_id})
    friend_ids = user["friends"]
    posts = list(db.posts.find({
        "tagged_user_ids": tagged_user_id,
        "author_id": {"$in": friend_ids},
    }))
    # Resolve author usernames
    author_ids = {p["author_id"] for p in posts}
    authors = {u["_id"]: u["username"] for u in db.users.find({"_id": {"$in": list(author_ids)}})}
    return [
        {"_id": p["_id"], "content": p["content"], "author": authors.get(p["author_id"], "unknown")}
        for p in posts
    ]
