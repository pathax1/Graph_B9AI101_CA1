from neo4j import GraphDatabase
import os
import csv

class LuasExecution:
    def __init__(self, uri, user, password):
        """Initialize the Neo4j connection"""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """Close the Neo4j connection"""
        if self.driver:
            self.driver.close()

    def execute_query(self, query, parameters=None):
        """Execute a given Cypher query."""
        with self.driver.session() as session:
            try:
                result = session.run(query, parameters)
                return [record for record in result]
            except Exception as e:
                print(f"Query execution failed: {e}")

    def import_luas_data(self, csv_file_path):
        """
        Import station data for the DART node from a CSV file.
        """
        if not os.path.exists(csv_file_path):
            print(f"CSV file not found at path: {csv_file_path}")
            return

        with open(csv_file_path, mode='r', encoding='latin1') as file:
            reader = csv.DictReader(file)
            for row in reader:
                query = """
                MATCH (luas:Category {name: 'LUAS'})
                 CREATE (station:Station 
                 {
                    name: $Station_Name,
                    Line: $Line,
                    Station_ID: $Station_ID,
                    Location: $Location,
                    Key_Features_Attractions: $Key_Features_Attractions,
                    Type: $Type,
                    Interchange: $Interchange,
                    Zone: $Zone,
                    Daily_Footfall: $Daily_Footfall,
                    Facilities: $Facilities,
                    Accessibility: $Accessibility,
                    Latitude: $Latitude,
                    Longitude: $Longitude,
                    Parking_Availability: $Parking_Availability,
                    Nearby_Landmarks: $Nearby_Landmarks,
                    First_Tram_Time: $First_Tram_Time,
                    Last_Tram_Time: $Last_Tram_Time
                })
                CREATE (luas)-[:HAS_STATION]->(station)
                """
                self.execute_query(
                    query,
                    parameters={
                        "Station_Name": row["Station Name"],
                        "Station_ID": row["Station_ID"],
                        "Line": row["Line"],
                        "Location": row["Location"],
                        "Key_Features_Attractions": row["Key Features/Attractions"],
                        "Type": row["Type (Terminus/Regular)"],
                        "Interchange": row["Interchange"],
                        "Zone": row["Zone"],
                        "Daily_Footfall": row["Daily Footfall"],
                        "Facilities": row["Facilities"],
                        "Accessibility": row["Accessibility"],
                        "Latitude": row["Latitude"],
                        "Longitude": row["Longitude"],
                        "Parking_Availability": row["Parking Availability"],
                        "Nearby_Landmarks": row["Nearby Landmarks"],
                        "First_Tram_Time": row["First Tram Time"],
                        "Last_Tram_Time": row["Last Tram Time"],
                    },
                )
        print("LUAS imported successfully!")

    def create_luas_station_relationships(self):
        """
        Create relationships between LUAS stations based on shared line.
        """
        query = """
        MATCH (station1:Station), (station2:Station)
        WHERE station1 <> station2
        AND station1.Line = station2.Line
        CREATE (station1)-[:CONNECTED_TO {travel_time: 5}]->(station2)
        """
        self.execute_query(query)
        print("Relationships between LUAS stations created based on shared line.")

    def create_interchange_relationships(self):
        """
        Create relationships between stations that are interchanges.
        """
        query = """
        MATCH (station1:Station), (station2:Station)
        WHERE station1 <> station2
        AND station1.Interchange = 'Yes'
        AND station2.Interchange = 'Yes'
        CREATE (station1)-[:INTERCHANGE {travel_time: 10}]->(station2)
        """
        self.execute_query(query)
        print("Interchange relationships created between LUAS stations.")