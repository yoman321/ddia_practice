"""
GraphQL schema that abstracts all three backends (relational, document, graph).

Each query takes a `backend` argument ("relational", "document", "graph")
so the same schema dispatches to the chosen data model.

Exposed queries:
  - friendsOfFriends(backend, userId)
  - postsByUser(backend, userId)
  - postsTaggingUserByFriendsOf(backend, taggedUserId, userId)
"""

import graphene

# ── Backend imports ───────────────────────────────────────────
import relational_db
import document_db
import graph_db

# Keep a single GraphDB instance alive for the document & graph backends.
# Relational backend re-inits from SQLite each call.
_graph = None
_doc_ids = {}
_rel_ids = {}


def _init_relational():
    import os
    if os.path.exists(relational_db.DATABASE):
        os.remove(relational_db.DATABASE)
    relational_db.init_db()

    alice = relational_db.create_user("alice", "Alice Smith", "alice@example.com", bio="Loves distributed systems")
    bob = relational_db.create_user("bob", "Bob Jones", "bob@example.com", bio="Graph enthusiast")
    carol = relational_db.create_user("carol", "Carol Lee", "carol@example.com", bio="Document model fan")
    dave = relational_db.create_user("dave", "Dave Kim", "dave@example.com", bio="New here")

    relational_db.add_friendship(alice, bob)
    relational_db.add_friendship(alice, carol)
    relational_db.add_friendship(bob, dave)

    p1 = relational_db.create_post(bob, "Just finished DDIA chapter 3 with @alice and @carol!")
    p2 = relational_db.create_post(carol, "Document databases are underrated.")
    relational_db.create_comment(p1, alice, "Great chapter on storage engines!")
    relational_db.create_comment(p1, carol, "LSM-trees are fascinating.")
    relational_db.tag_user(p1, alice)
    relational_db.tag_user(p1, carol)
    relational_db.tag_user(p2, dave)

    _rel_ids["alice"] = alice
    _rel_ids["bob"] = bob
    _rel_ids["carol"] = carol
    _rel_ids["dave"] = dave


def _init_document():
    document_db._collections["users"] = {}
    document_db._next_id = 1

    alice = document_db.create_user("alice", "Alice Smith", "alice@example.com", bio="Loves distributed systems")
    bob = document_db.create_user("bob", "Bob Jones", "bob@example.com", bio="Graph enthusiast")
    carol = document_db.create_user("carol", "Carol Lee", "carol@example.com", bio="Document model fan")
    dave = document_db.create_user("dave", "Dave Kim", "dave@example.com", bio="New here")

    document_db.add_friendship(alice, bob)
    document_db.add_friendship(alice, carol)
    document_db.add_friendship(bob, dave)

    p1 = document_db.create_post(bob, "Just finished DDIA chapter 3 with @alice and @carol!")
    p2 = document_db.create_post(carol, "Document databases are underrated.")
    document_db.create_comment(alice, bob, p1, "Great chapter on storage engines!")
    document_db.create_comment(carol, bob, p1, "LSM-trees are fascinating.")
    document_db.tag_user(bob, p1, alice)
    document_db.tag_user(bob, p1, carol)
    document_db.tag_user(carol, p2, dave)

    _doc_ids["alice"] = alice
    _doc_ids["bob"] = bob
    _doc_ids["carol"] = carol
    _doc_ids["dave"] = dave


def _init_graph():
    global _graph
    _graph = graph_db.GraphDB()

    alice = _graph.create_user("alice", "Alice Smith", "alice@example.com", bio="Loves distributed systems")
    bob = _graph.create_user("bob", "Bob Jones", "bob@example.com", bio="Graph enthusiast")
    carol = _graph.create_user("carol", "Carol Lee", "carol@example.com", bio="Document model fan")
    dave = _graph.create_user("dave", "Dave Kim", "dave@example.com", bio="New here")

    _graph.add_friendship(alice, bob)
    _graph.add_friendship(alice, carol)
    _graph.add_friendship(bob, dave)

    p1 = _graph.create_post(bob, "Just finished DDIA chapter 3 with @alice and @carol!")
    p2 = _graph.create_post(carol, "Document databases are underrated.")
    _graph.create_comment(alice, p1, "Great chapter on storage engines!")
    _graph.create_comment(carol, p1, "LSM-trees are fascinating.")
    _graph.tag_user(p1, alice)
    _graph.tag_user(p1, carol)
    _graph.tag_user(p2, dave)

    _doc_ids["g_alice"] = alice
    _doc_ids["g_bob"] = bob
    _doc_ids["g_carol"] = carol
    _doc_ids["g_dave"] = dave


def _seed_all():
    _init_relational()
    _init_document()
    _init_graph()


# ── GraphQL types ─────────────────────────────────────────────

class UserType(graphene.ObjectType):
    id = graphene.String()
    username = graphene.String()
    name = graphene.String()


class PostType(graphene.ObjectType):
    id = graphene.String()
    content = graphene.String()
    author = graphene.String()
    created_at = graphene.String()


class BackendEnum(graphene.Enum):
    RELATIONAL = "relational"
    DOCUMENT = "document"
    GRAPH = "graph"


# ── Resolvers ─────────────────────────────────────────────────

def _resolve_user_id(backend, username):
    """Map a username to the backend-specific user ID."""
    if backend == "relational":
        conn = relational_db.get_db_connection()
        row = conn.execute(
            "SELECT id FROM users WHERE username = ?", (username,)
        ).fetchone()
        conn.close()
        return row["id"] if row else None
    elif backend == "document":
        for uid, doc in document_db._collections["users"].items():
            if doc["username"] == username:
                return uid
        return None
    elif backend == "graph":
        for vid, v in _graph.vertices.items():
            if v.label == "User" and v.properties.get("username") == username:
                return vid
        return None


class Query(graphene.ObjectType):
    friends_of_friends = graphene.List(
        UserType,
        backend=BackendEnum(required=True),
        username=graphene.String(required=True),
        description="Friends-of-friends of user X (2-hop discovery).",
    )
    posts_by_user = graphene.List(
        PostType,
        backend=BackendEnum(required=True),
        username=graphene.String(required=True),
        description="All posts authored by user X.",
    )
    posts_tagging_user_by_friends_of = graphene.List(
        PostType,
        backend=BackendEnum(required=True),
        tagged_username=graphene.String(required=True),
        username=graphene.String(required=True),
        description="Posts that tag user Y, authored by friends of user X.",
    )

    def resolve_friends_of_friends(root, info, backend, username):
        uid = _resolve_user_id(backend, username)
        if uid is None:
            return []
        if backend == "relational":
            rows = relational_db.get_friends_of_friends(uid)
            return [UserType(id=r["id"], username=r["username"], name=r["name"]) for r in rows]
        elif backend == "document":
            rows = document_db.get_friends_of_friends(uid)
            return [UserType(id=r["_id"], username=r["username"], name=r["name"]) for r in rows]
        elif backend == "graph":
            rows = _graph.get_friends_of_friends(uid)
            return [UserType(id=r["id"], username=r["username"], name=r["name"]) for r in rows]

    def resolve_posts_by_user(root, info, backend, username):
        uid = _resolve_user_id(backend, username)
        if uid is None:
            return []
        if backend == "relational":
            rows = relational_db.get_posts_by_user(uid)
        elif backend == "document":
            rows = document_db.get_posts_by_user(uid)
        elif backend == "graph":
            rows = _graph.get_posts_by_user(uid)
        return [PostType(id=r.get("id") or r.get("_id"), content=r["content"],
                         author=r["author"], created_at=r["created_at"]) for r in rows]

    def resolve_posts_tagging_user_by_friends_of(root, info, backend, tagged_username, username):
        uid = _resolve_user_id(backend, username)
        tagged_uid = _resolve_user_id(backend, tagged_username)
        if uid is None or tagged_uid is None:
            return []
        if backend == "relational":
            rows = relational_db.get_posts_tagging_user_by_friends_of(tagged_uid, uid)
        elif backend == "document":
            rows = document_db.get_posts_tagging_user_by_friends_of(tagged_uid, uid)
        elif backend == "graph":
            rows = _graph.get_posts_tagging_user_by_friends_of(tagged_uid, uid)
        return [PostType(id=r.get("id") or r.get("_id"), content=r["content"],
                         author=r["author"], created_at=r["created_at"]) for r in rows]


schema = graphene.Schema(query=Query)


# ── Flask app ─────────────────────────────────────────────────

GRAPHIQL_HTML = """
<!DOCTYPE html>
<html>
<head>
  <title>Social Network GraphQL</title>
  <link rel="stylesheet" href="https://unpkg.com/graphiql@3.0.6/graphiql.min.css" />
</head>
<body style="margin:0;">
  <div id="graphiql" style="height:100vh;"></div>
  <script crossorigin src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/graphiql@3.0.6/graphiql.min.js"></script>
  <script>
    const root = ReactDOM.createRoot(document.getElementById('graphiql'));
    const fetcher = GraphiQL.createFetcher({ url: '/graphql' });
    root.render(React.createElement(GraphiQL, { fetcher }));
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    from flask import Flask, request, jsonify, make_response

    _seed_all()

    app = Flask(__name__)

    @app.route("/graphql", methods=["GET"])
    def graphiql():
        return make_response(GRAPHIQL_HTML)

    @app.route("/graphql", methods=["POST"])
    def graphql_endpoint():
        data = request.get_json()
        result = schema.execute(
            data.get("query", ""),
            variables=data.get("variables"),
            operation_name=data.get("operationName"),
        )
        response = {"data": result.data}
        if result.errors:
            response["errors"] = [str(e) for e in result.errors]
        return jsonify(response)

    print("GraphiQL available at http://127.0.0.1:5010/graphql")
    print("\nExample queries:")
    print("""
  {
    friendsOfFriends(backend: RELATIONAL, username: "alice") {
      id username name
    }
  }

  {
    postsByUser(backend: DOCUMENT, username: "bob") {
      id content author createdAt
    }
  }

  {
    postsTaggingUserByFriendsOf(backend: GRAPH, taggedUsername: "carol", username: "alice") {
      id content author createdAt
    }
  }
""")
    app.run(debug=True, port=5010)
