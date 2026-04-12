"""
Social network schema — Graph model (in-memory adjacency list).

Vertices: User, Post, Comment
Edges:    FRIENDS_WITH, AUTHORED, COMMENTED_ON, TAGGED

Property graph: each vertex and edge carries a dict of properties.
TAGGED edges go from a Post to the User being mentioned.
Demonstrates traversal-based queries natural to graph databases.
"""

from collections import deque
from datetime import datetime


class Vertex:
    def __init__(self, vid, label, properties=None):
        self.id = vid
        self.label = label          # "User", "Post", "Comment"
        self.properties = properties or {}
        self.out_edges: list[Edge] = []
        self.in_edges: list[Edge] = []

    def __repr__(self):
        return f"Vertex({self.label}:{self.id}, {self.properties})"


class Edge:
    def __init__(self, eid, label, src: Vertex, dst: Vertex, properties=None):
        self.id = eid
        self.label = label          # "FRIENDS_WITH", "AUTHORED", "COMMENTED_ON", "TAGGED"
        self.src = src
        self.dst = dst
        self.properties = properties or {}

    def __repr__(self):
        return f"Edge({self.src.id})-[{self.label}]->({self.dst.id})"


class GraphDB:
    def __init__(self):
        self.vertices: dict[str, Vertex] = {}
        self.edges: list[Edge] = []
        self._next_id = 1

    def _new_id(self):
        _id = str(self._next_id)
        self._next_id += 1
        return _id

    # ── Mutations ─────────────────────────────────────────────

    def add_vertex(self, label, properties=None):
        vid = self._new_id()
        v = Vertex(vid, label, {
            **(properties or {}),
            "created_at": datetime.now().isoformat(),
        })
        self.vertices[vid] = v
        return vid

    def add_edge(self, label, src_id, dst_id, properties=None):
        eid = self._new_id()
        src = self.vertices[src_id]
        dst = self.vertices[dst_id]
        e = Edge(eid, label, src, dst, properties or {})
        self.edges.append(e)
        src.out_edges.append(e)
        dst.in_edges.append(e)
        return eid

    # ── Convenience builders ──────────────────────────────────

    def create_user(self, username, name, email, bio=""):
        return self.add_vertex("User", {
            "username": username, "name": name, "email": email, "bio": bio,
        })

    def add_friendship(self, uid1, uid2):
        self.add_edge("FRIENDS_WITH", uid1, uid2)
        self.add_edge("FRIENDS_WITH", uid2, uid1)

    def create_post(self, author_id, content):
        pid = self.add_vertex("Post", {"content": content})
        self.add_edge("AUTHORED", author_id, pid)
        return pid

    def create_comment(self, author_id, post_id, content):
        cid = self.add_vertex("Comment", {"content": content})
        self.add_edge("AUTHORED", author_id, cid)
        self.add_edge("COMMENTED_ON", cid, post_id)
        return cid

    def tag_user(self, post_id, tagged_user_id):
        """Post -TAGGED-> User (mention)."""
        self.add_edge("TAGGED", post_id, tagged_user_id)

    # ── Traversal helpers ─────────────────────────────────────

    def _outgoing(self, vid, edge_label):
        return [e.dst for e in self.vertices[vid].out_edges if e.label == edge_label]

    def _incoming(self, vid, edge_label):
        return [e.src for e in self.vertices[vid].in_edges if e.label == edge_label]

    # ── Queries ───────────────────────────────────────────────

    def get_friends(self, user_id):
        return [
            {"id": v.id, "username": v.properties["username"], "name": v.properties["name"]}
            for v in self._outgoing(user_id, "FRIENDS_WITH")
        ]

    def get_feed(self, user_id):
        """Traverse: user -FRIENDS_WITH-> friend -AUTHORED-> post."""
        posts = []
        for friend in self._outgoing(user_id, "FRIENDS_WITH"):
            for post in self._outgoing(friend.id, "AUTHORED"):
                if post.label == "Post":
                    posts.append({
                        "id": post.id,
                        "content": post.properties["content"],
                        "author": friend.properties["username"],
                        "created_at": post.properties["created_at"],
                    })
        posts.sort(key=lambda p: p["created_at"], reverse=True)
        return posts

    def get_posts_by_user(self, user_id):
        """Traverse: user -AUTHORED-> post."""
        posts = []
        for v in self._outgoing(user_id, "AUTHORED"):
            if v.label == "Post":
                posts.append({
                    "id": v.id,
                    "content": v.properties["content"],
                    "author": self.vertices[user_id].properties["username"],
                    "created_at": v.properties["created_at"],
                })
        posts.sort(key=lambda p: p["created_at"], reverse=True)
        return posts

    def get_friends_of_friends(self, user_id):
        """2-hop: user -> friend -> friend-of-friend (excluding direct friends and self)."""
        direct = {v.id for v in self._outgoing(user_id, "FRIENDS_WITH")}
        suggestions = {}
        for friend in self._outgoing(user_id, "FRIENDS_WITH"):
            for fof in self._outgoing(friend.id, "FRIENDS_WITH"):
                if fof.id != user_id and fof.id not in direct:
                    suggestions[fof.id] = {
                        "id": fof.id,
                        "username": fof.properties["username"],
                        "name": fof.properties["name"],
                    }
        return list(suggestions.values())

    def get_posts_tagging_user(self, user_id):
        """Traverse incoming TAGGED edges on a User vertex to find posts mentioning them."""
        posts = self._incoming(user_id, "TAGGED")
        results = []
        for post in posts:
            authors = self._incoming(post.id, "AUTHORED")
            author_name = authors[0].properties["username"] if authors else "unknown"
            results.append({
                "id": post.id,
                "content": post.properties["content"],
                "author": author_name,
                "created_at": post.properties["created_at"],
            })
        return results

    def get_posts_tagging_user_by_friends_of(self, tagged_user_id, user_id):
        """Posts that tag user Y, authored by friends of user X.
        Traverse: user_x -FRIENDS_WITH-> friend -AUTHORED-> post -TAGGED-> user_y."""
        friend_ids = {v.id for v in self._outgoing(user_id, "FRIENDS_WITH")}
        tagged_posts = self._incoming(tagged_user_id, "TAGGED")
        results = []
        for post in tagged_posts:
            authors = self._incoming(post.id, "AUTHORED")
            for author in authors:
                if author.id in friend_ids:
                    results.append({
                        "id": post.id,
                        "content": post.properties["content"],
                        "author": author.properties["username"],
                        "created_at": post.properties["created_at"],
                    })
        results.sort(key=lambda p: p["created_at"], reverse=True)
        return results

    def get_post_with_details(self, post_id):
        post = self.vertices[post_id]
        authors = self._incoming(post_id, "AUTHORED")
        author_name = authors[0].properties["username"] if authors else "unknown"

        comment_vertices = self._incoming(post_id, "COMMENTED_ON")
        comments = []
        for cv in comment_vertices:
            ca = self._incoming(cv.id, "AUTHORED")
            comments.append({
                "content": cv.properties["content"],
                "author": ca[0].properties["username"] if ca else "unknown",
                "created_at": cv.properties["created_at"],
            })

        tagged_users = [
            {"id": v.id, "username": v.properties["username"]}
            for v in self._outgoing(post_id, "TAGGED")
        ]

        return {
            "id": post_id,
            "content": post.properties["content"],
            "author": author_name,
            "comments": comments,
            "tagged_users": tagged_users,
            "created_at": post.properties["created_at"],
        }

    def shortest_path(self, start_id, end_id):
        """BFS shortest path between any two vertices (unweighted)."""
        visited = {start_id}
        queue = deque([(start_id, [start_id])])
        while queue:
            current, path = queue.popleft()
            if current == end_id:
                return path
            for e in self.vertices[current].out_edges:
                nid = e.dst.id
                if nid not in visited:
                    visited.add(nid)
                    queue.append((nid, path + [nid]))
        return None


# ── Demo ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    g = GraphDB()

    alice = g.create_user("alice", "Alice Smith", "alice@example.com", bio="Loves distributed systems")
    bob = g.create_user("bob", "Bob Jones", "bob@example.com", bio="Graph enthusiast")
    carol = g.create_user("carol", "Carol Lee", "carol@example.com", bio="Document model fan")
    dave = g.create_user("dave", "Dave Kim", "dave@example.com", bio="New here")

    g.add_friendship(alice, bob)
    g.add_friendship(alice, carol)
    g.add_friendship(bob, dave)

    p1 = g.create_post(bob, "Just finished DDIA chapter 3 with @alice and @carol!")
    p2 = g.create_post(carol, "Document databases are underrated.")
    g.create_comment(alice, p1, "Great chapter on storage engines!")
    g.create_comment(carol, p1, "LSM-trees are fascinating.")
    g.tag_user(p1, alice)
    g.tag_user(p1, carol)
    g.tag_user(p2, dave)

    def _pp(obj):
        print(json.dumps(obj, indent=2, default=str))

    print("=== Graph (Adjacency List) ===\n")
    print("Alice's friends:")
    _pp(g.get_friends(alice))
    print("\nAlice's feed:")
    _pp(g.get_feed(alice))
    print("\nAlice's friend suggestions:")
    _pp(g.get_friends_of_friends(alice))
    print("\nPost details:")
    _pp(g.get_post_with_details(p1))
    print("\nBob's posts:")
    _pp(g.get_posts_by_user(bob))
    print("\nPosts tagging Alice:")
    _pp(g.get_posts_tagging_user(alice))

    print("\nPosts tagging Carol by Alice's friends:")
    _pp(g.get_posts_tagging_user_by_friends_of(carol, alice))

    path = g.shortest_path(alice, dave)
    print(f"\nShortest path Alice -> Dave: {path}")
    if path:
        print("  Hops: " + " -> ".join(
            g.vertices[vid].properties.get("username", g.vertices[vid].label)
            for vid in path
        ))
