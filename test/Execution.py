from neo4j import GraphDatabase
import os

from CRISP_DM.EDA import Neo4jEDA
from pages.Bus import BusExecution
from pages.Dart import DartExecution
from pages.Luas import LuasExecution
from pages.Master import MasterNode


class Neo4jExecution:
    def __init__(self, uri, user, password):
        """Initialize the Neo4j connection"""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """Close the Neo4j connection"""
        if self.driver:
            self.driver.close()

    def iconnect(self):
        """Test if the connection to the Neo4j server is established."""
        try:
            with self.driver.session() as session:
                session.run("RETURN 1")
            print("Connection to Neo4j established successfully!")
        except Exception as e:
            print("Failed to connect to Neo4j:", e)

    def execute_query(self, query, parameters=None):
        """Execute a given Cypher query."""
        with self.driver.session() as session:
            try:
                result = session.run(query, parameters)
                return [record for record in result]
            except Exception as e:
                print(f"Query execution failed: {e}")


# Main Execution
if __name__ == "__main__":
    # Define Neo4j connection details
    URI = "bolt://localhost:7687"
    USER = "neo4j"
    PASSWORD = "9820065151"

    # Path to the CSV file
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DART_CSV_FILE_PATH = os.path.join(PROJECT_ROOT, "data","DART_Dataset.csv")
    LUAS_CSV_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "LUAS_Dataset.csv")
    BUS_CSV_FILE_PATH = os.path.join(PROJECT_ROOT, "data", "BUS_Dataset.csv")

    # Create an instance of the Neo4jExecution class
    neo4j_exec = Neo4jExecution(URI, USER, PASSWORD)

    try:
        # Test the connection
        neo4j_exec.iconnect()
        # Create master-parent-child nodes
        imaster = MasterNode(URI, USER, PASSWORD)
        imaster.create_master_parent_child_node()

        # Import station data from CSV
        iDart=DartExecution(URI, USER, PASSWORD)
        iDart.import_station_data(DART_CSV_FILE_PATH)
        iDart.create_station_relationships()

        iluas=LuasExecution(URI, USER, PASSWORD)
        iluas.import_luas_data(LUAS_CSV_FILE_PATH)
        iluas.create_luas_station_relationships()
        iluas.create_interchange_relationships()

        ibus=BusExecution(URI, USER, PASSWORD)
        ibus.import_bus_data(BUS_CSV_FILE_PATH)
        ibus.create_route_relationships()
        ibus.create_route_connections()

        ieda = Neo4jEDA(URI, USER, PASSWORD)
        ieda.test_connection()


        ieda.count_nodes_and_relationships()
        print(ieda.count_nodes_and_relationships())


        ieda.get_node_labels()
        # Get node labels
        print("\nNode Labels:")
        print(ieda.get_node_labels())


        ieda.get_relationship_types()
        # Get relationship types
        print("\nRelationship Types:")
        print(ieda.get_relationship_types())


        ieda.most_connected_nodes()
        # Get most connected nodes
        print("\nMost Connected Nodes:")
        print(ieda.most_connected_nodes(limit=5))

        ieda.degree_distribution()
        # Get degree distribution
        print("\nDegree Distribution:")
        print(ieda.degree_distribution(limit=5))

        #ieda.delete_existing_graph("luasGraph")
        ieda.sample_subgraph()
        degree_data = ieda.degree_distribution(limit=20)
        ieda.visualize_node_degree_distribution(degree_data)
        ieda.apply_graph_algorithms()
    finally:
        # Close the connection
        neo4j_exec.close()
