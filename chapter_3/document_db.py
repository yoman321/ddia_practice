"""
Social network schema — Document model (MongoDB-style, in-memory).

Each user document embeds their posts, comments, and friend list.
Tags are stored as arrays of user IDs inside each post subdocument,
representing user mentions. Demonstrates denormalized, nested structure
typical of document stores.
"""

import json
from datetime import datetime

# ── In-memory document store ──────────────────────────────────
# Simulates a collection-based document DB using plain dicts.
# Each collection is a dict mapping string IDs to documents.

_collections: dict[str, dict[str, dict]] = {
    "users": {},
}

_next_id = 1


def _new_id():
    global _next_id
    _id = str(_next_id)
    _next_id += 1
    return _id


def _now():
    return datetime.now().isoformat()


# ── CRUD helpers ──────────────────────────────────────────────

def create_user(username, name, email, bio=""):
    uid = _new_id()
    _collections["users"][uid] = {
        "_id": uid,
        "username": username,
        "name": name,
        "email": email,
        "bio": bio,
        "friends": [],          # list of user IDs
        "posts": [],            # embedded post subdocuments
        "created_at": _now(),
    }
    return uid


def add_friendship(user_id, friend_id):
    """Bidirectional — each user's document stores the other's ID."""
    u = _collections["users"][user_id]
    f = _collections["users"][friend_id]
    if friend_id not in u["friends"]:
        u["friends"].append(friend_id)
    if user_id not in f["friends"]:
        f["friends"].append(user_id)


def create_post(author_id, content):
    """Embeds the post inside the author's user document."""
    pid = _new_id()
    post = {
        "_id": pid,
        "content": content,
        "tagged_user_ids": [],   # list of user IDs mentioned in this post
        "comments": [],
        "created_at": _now(),
    }
    _collections["users"][author_id]["posts"].append(post)
    return pid


def tag_user(author_id, post_id, tagged_user_id):
    """Tag (mention) a user in a post."""
    user = _collections["users"][author_id]
    for post in user["posts"]:
        if post["_id"] == post_id:
            if tagged_user_id not in post["tagged_user_ids"]:
                post["tagged_user_ids"].append(tagged_user_id)
            return


def create_comment(author_id, post_owner_id, post_id, content):
    """Embeds comment inside the post subdocument of the post owner."""
    cid = _new_id()
    owner = _collections["users"][post_owner_id]
    for post in owner["posts"]:
        if post["_id"] == post_id:
            post["comments"].append({
                "_id": cid,
                "author_id": author_id,
                "author_username": _collections["users"][author_id]["username"],
                "content": content,
                "created_at": _now(),
            })
            return cid
    return None


# ── Queries ───────────────────────────────────────────────────

def get_user(user_id):
    return _collections["users"].get(user_id)


def get_friends(user_id):
    user = _collections["users"][user_id]
    return [
        {"_id": fid, "username": _collections["users"][fid]["username"],
         "name": _collections["users"][fid]["name"]}
        for fid in user["friends"]
    ]


def get_feed(user_id):
    """Aggregate posts from all friends (scan their embedded posts)."""
    user = _collections["users"][user_id]
    feed = []
    for fid in user["friends"]:
        friend = _collections["users"][fid]
        for post in friend["posts"]:
            feed.append({
                **post,
                "author": friend["username"],
            })
    feed.sort(key=lambda p: p["created_at"], reverse=True)
    return feed


def get_posts_by_user(user_id):
    """Find all posts authored by a given user — single document read."""
    user = _collections["users"][user_id]
    return [
        {**post, "author": user["username"]}
        for post in sorted(user["posts"], key=lambda p: p["created_at"], reverse=True)
    ]


def get_friends_of_friends(user_id):
    """2-hop friend discovery."""
    user = _collections["users"][user_id]
    direct = set(user["friends"])
    suggestions = {}
    for fid in user["friends"]:
        friend = _collections["users"][fid]
        for fof_id in friend["friends"]:
            if fof_id != user_id and fof_id not in direct:
                fof = _collections["users"][fof_id]
                suggestions[fof_id] = {
                    "_id": fof_id,
                    "username": fof["username"],
                    "name": fof["name"],
                }
    return list(suggestions.values())


def get_posts_tagging_user(tagged_user_id):
    """Scan all users' posts for mentions of a given user (no index)."""
    results = []
    for user in _collections["users"].values():
        for post in user["posts"]:
            if tagged_user_id in post["tagged_user_ids"]:
                results.append({
                    **post,
                    "author": user["username"],
                })
    results.sort(key=lambda p: p["created_at"], reverse=True)
    return results


def get_posts_tagging_user_by_friends_of(tagged_user_id, user_id):
    """Posts that tag user Y, authored by friends of user X.
    Requires scanning friend documents for posts that mention Y."""
    user = _collections["users"][user_id]
    results = []
    for fid in user["friends"]:
        friend = _collections["users"][fid]
        for post in friend["posts"]:
            if tagged_user_id in post["tagged_user_ids"]:
                results.append({
                    **post,
                    "author": friend["username"],
                })
    results.sort(key=lambda p: p["created_at"], reverse=True)
    return results


def get_post_with_details(post_owner_id, post_id):
    owner = _collections["users"][post_owner_id]
    for post in owner["posts"]:
        if post["_id"] == post_id:
            tagged_users = [
                {"_id": uid, "username": _collections["users"][uid]["username"]}
                for uid in post["tagged_user_ids"]
            ]
            return {
                **post,
                "author": owner["username"],
                "tagged_users": tagged_users,
            }
    return None


# ── Demo ──────────────────────────────────────────────────────

def _pp(obj):
    print(json.dumps(obj, indent=2, default=str))


if __name__ == "__main__":
    alice = create_user("alice", "Alice Smith", "alice@example.com", bio="Loves distributed systems")
    bob = create_user("bob", "Bob Jones", "bob@example.com", bio="Graph enthusiast")
    carol = create_user("carol", "Carol Lee", "carol@example.com", bio="Document model fan")
    dave = create_user("dave", "Dave Kim", "dave@example.com", bio="New here")

    add_friendship(alice, bob)
    add_friendship(alice, carol)
    add_friendship(bob, dave)

    p1 = create_post(bob, "Just finished DDIA chapter 3 with @alice and @carol!")
    p2 = create_post(carol, "Document databases are underrated.")
    create_comment(alice, bob, p1, "Great chapter on storage engines!")
    create_comment(carol, bob, p1, "LSM-trees are fascinating.")
    tag_user(bob, p1, alice)
    tag_user(bob, p1, carol)
    tag_user(carol, p2, dave)

    print("=== Document (MongoDB-style) ===\n")
    print("Alice's friends:")
    _pp(get_friends(alice))
    print("\nAlice's feed:")
    _pp(get_feed(alice))
    print("\nAlice's friend suggestions:")
    _pp(get_friends_of_friends(alice))
    print("\nPost details:")
    _pp(get_post_with_details(bob, p1))
    print("\nBob's posts:")
    _pp(get_posts_by_user(bob))
    print("\nPosts tagging Alice:")
    _pp(get_posts_tagging_user(alice))
    print("\nPosts tagging Carol by Alice's friends:")
    _pp(get_posts_tagging_user_by_friends_of(carol, alice))
    print("\nFull Bob document:")
    _pp(get_user(bob))
