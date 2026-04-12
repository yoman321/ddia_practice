"""
Client for the SQLite server — same interface as mongo_db / neo4j_db.
"""

import requests

BASE_URL = "http://127.0.0.1:5020"


def seed(num_users=1000, num_posts=10000):
    resp = requests.post(f"{BASE_URL}/seed", params={"num_users": num_users, "num_posts": num_posts})
    return resp.json()["user_ids"]


def get_friends_of_friends(user_id):
    resp = requests.get(f"{BASE_URL}/friends_of_friends", params={"user_id": user_id})
    return resp.json()


def get_posts_by_user(user_id):
    resp = requests.get(f"{BASE_URL}/posts_by_user", params={"user_id": user_id})
    return resp.json()


def get_posts_tagging_user_by_friends_of(tagged_user_id, user_id):
    resp = requests.get(f"{BASE_URL}/posts_tagging_user_by_friends_of",
                        params={"tagged_user_id": tagged_user_id, "user_id": user_id})
    return resp.json()
