"""
Social network schema — Graph model (Neo4j).

Nodes: User, Post
Relationships: FRIENDS_WITH, AUTHORED, TAGGED

Same queries as the in-memory graph_db, but backed by real Neo4j.
"""

from neo4j import GraphDatabase

import os

URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
AUTH = (os.environ.get("NEO4J_USER", "neo4j"), os.environ.get("NEO4J_PASSWORD", "testpassword"))

_driver = None


def get_driver():
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(URI, auth=AUTH)
    return _driver


def init_db():
    driver = get_driver()
    with driver.session() as s:
        s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.username IS UNIQUE")
        s.run("CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.uid)")


def drop_db():
    driver = get_driver()
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")


# ── CRUD helpers ──────────────────────────────────────────────

def create_user(uid, username, name, email, bio=""):
    driver = get_driver()
    with driver.session() as s:
        s.run(
            "CREATE (u:User {uid: $uid, username: $username, name: $name, email: $email, bio: $bio})",
            uid=uid, username=username, name=name, email=email, bio=bio,
        )
    return uid


def add_friendship(uid1, uid2):
    driver = get_driver()
    with driver.session() as s:
        s.run("""
            MATCH (a:User {uid: $uid1}), (b:User {uid: $uid2})
            MERGE (a)-[:FRIENDS_WITH]->(b)
            MERGE (b)-[:FRIENDS_WITH]->(a)
        """, uid1=uid1, uid2=uid2)


def create_post(pid, author_uid, content):
    driver = get_driver()
    with driver.session() as s:
        s.run("""
            MATCH (u:User {uid: $author_uid})
            CREATE (p:Post {pid: $pid, content: $content})
            CREATE (u)-[:AUTHORED]->(p)
        """, pid=pid, author_uid=author_uid, content=content)
    return pid


def tag_user(pid, tagged_uid):
    driver = get_driver()
    with driver.session() as s:
        s.run("""
            MATCH (p:Post {pid: $pid}), (u:User {uid: $tagged_uid})
            MERGE (p)-[:TAGGED]->(u)
        """, pid=pid, tagged_uid=tagged_uid)


# ── Queries ───────────────────────────────────────────────────

def get_friends_of_friends(uid):
    driver = get_driver()
    with driver.session() as s:
        result = s.run("""
            MATCH (u:User {uid: $uid})-[:FRIENDS_WITH]->()-[:FRIENDS_WITH]->(fof:User)
            WHERE fof.uid <> $uid
              AND NOT (u)-[:FRIENDS_WITH]->(fof)
            RETURN DISTINCT fof.uid AS id, fof.username AS username, fof.name AS name
        """, uid=uid)
        return [dict(r) for r in result]


def get_posts_by_user(uid):
    driver = get_driver()
    with driver.session() as s:
        result = s.run("""
            MATCH (u:User {uid: $uid})-[:AUTHORED]->(p:Post)
            RETURN p.pid AS id, p.content AS content, u.username AS author
        """, uid=uid)
        return [dict(r) for r in result]


def get_posts_tagging_user_by_friends_of(tagged_uid, uid):
    driver = get_driver()
    with driver.session() as s:
        result = s.run("""
            MATCH (x:User {uid: $uid})-[:FRIENDS_WITH]->(friend:User)-[:AUTHORED]->(p:Post)-[:TAGGED]->(tagged:User {uid: $tagged_uid})
            RETURN p.pid AS id, p.content AS content, friend.username AS author
        """, uid=uid, tagged_uid=tagged_uid)
        return [dict(r) for r in result]


def close():
    global _driver
    if _driver:
        _driver.close()
        _driver = None
