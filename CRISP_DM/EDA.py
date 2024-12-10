import networkx as nx
from matplotlib import pyplot as plt
from py2neo import Graph


class Neo4jEDA:
    def __init__(self, uri, username, password):
        """
        Initialize the connection to the Neo4j database.
        """
        self.driver = None
        self.graph = Graph(uri, auth=(username, password))

    def test_connection(self):
        """
        Test the connection to the Neo4j instance.
        """
        try:
            return self.graph.run("RETURN 'Connection Successful' AS message").data()
        except Exception as e:
            return {"error": str(e)}

    def count_nodes_and_relationships(self):
        """
        Count the total number of nodes and relationships in the graph.
        """
        nodes = self.graph.evaluate("MATCH (n) RETURN COUNT(n) AS count")
        relationships = self.graph.evaluate("MATCH ()-[r]->() RETURN COUNT(r) AS count")
        print("\nCount Nodes and Relationships:")
        return {"nodes": nodes, "relationships": relationships}

    def get_node_labels(self):
        """
        Get all distinct node labels in the graph.
        """
        labels = self.graph.run("CALL db.labels()").data()
        return [label['label'] for label in labels]

    def get_relationship_types(self):
        """
        Get all distinct relationship types in the graph.
        """
        rel_types = self.graph.run("CALL db.relationshipTypes()").data()
        return [rel_type['relationshipType'] for rel_type in rel_types]

    def most_connected_nodes(self, limit=10):
        """
        Get the top `limit` most connected nodes in the graph.
        """
        query = f"""
        MATCH (n)-[r]->()
        RETURN n, COUNT(r) AS connections
        ORDER BY connections DESC
        LIMIT {limit}
        """
        results = self.graph.run(query).data()
        return results

    def degree_distribution(self, limit=20):
        """
        Get the degree distribution of nodes, limited to top `limit` nodes.
        """
        query = f"""
        MATCH (n)
        RETURN labels(n) AS labels, COUNT {{ (n)--() }} AS degree
        ORDER BY degree DESC
        LIMIT {limit}
        """
        results = self.graph.run(query).data()
        return results

    def sample_subgraph(self, label=None, limit=10):
        """
        Fetch a sample subgraph. Optionally filter by a node label.
        """
        if label:
            query = f"""
            MATCH (n:{label})-[r]-(m)
            RETURN n, r, m
            LIMIT {limit}
            """
        else:
            query = f"""
            MATCH (n)-[r]-(m)
            RETURN n, r, m
            LIMIT {limit}
            """
        results = self.graph.run(query).data()
        return results

    def delete_existing_graph(self, graph_name):
        with self.driver.session() as session:
            try:
                # Check if the graph exists
                exists = session.run(
                    "CALL gds.graph.exists($graph_name) YIELD exists RETURN exists",
                    graph_name=graph_name
                ).single()
                if exists and exists["exists"]:
                    # If the graph exists, drop it
                    session.run("CALL gds.graph.drop($graph_name)", graph_name=graph_name)
                    print(f"Graph '{graph_name}' deleted successfully.")
                else:
                    print(f"Graph '{graph_name}' does not exist.")
            except Exception as e:
                print(f"An error occurred: {e}")
    # Visualization Functions
    def visualize_node_degree_distribution(self, degree_data):
        """
        Visualize the degree distribution of nodes as a bar chart.
        """
        labels = [" / ".join(d['labels']) if d['labels'] else "No Label" for d in degree_data]
        degrees = [d['degree'] for d in degree_data]

        plt.figure(figsize=(10, 6))
        plt.barh(labels, degrees, color='skyblue')
        plt.xlabel('Degree (Number of Connections)')
        plt.ylabel('Node Labels')
        plt.title('Node Degree Distribution')
        plt.gca().invert_yaxis()
        plt.show()

    def apply_graph_algorithms(self):
        """
        Apply graph algorithms including graph projection and shortest path analysis.
        """
        print("\nProjecting the graph for analysis:")
        try:
            graph_projection_query = """
            CALL gds.graph.project(
                'luasGraph',
                'Station',
                {
                    CONNECTED_TO: {
                        type: 'CONNECTED_TO',
                        properties: 'travel_time'
                    }
                }
            );
            """
            self.graph.run(graph_projection_query)
            print("Graph projected successfully.")
        except Exception as e:
            print(f"Error during graph projection: {e}")

        print("\nFinding shortest path between two stations:")
        try:
            shortest_path_query = """
            MATCH (start:Station {name: $startStation}), (end:Station {name: $endStation})
            CALL gds.shortestPath.dijkstra.stream('luasGraph', {
                sourceNode: id(start),
                targetNode: id(end)
            })
            YIELD totalCost, nodeIds
            RETURN totalCost, [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS path;
            """
            shortest_path_params = {
                "startStation": "Tallaght",  # Replace with actual station name
                "endStation": "Heuston"  # Replace with actual station name
            }
            shortest_path_result = self.graph.run(shortest_path_query, shortest_path_params).data()
            print("Shortest Path Results:", shortest_path_result)
        except Exception as e:
            print(f"Error during shortest path calculation: {e}")

# Example Usage
if __name__ == "__main__":
    eda = Neo4jEDA("bolt://localhost:7687", "neo4j", "9820065151")
    eda.test_connection()
    eda.apply_graph_algorithms()
