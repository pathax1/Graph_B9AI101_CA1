from neo4j import GraphDatabase
import os
import csv

class DartExecution:
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

    def import_station_data(self, csv_file_path):
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
                MATCH (dart:Category {name: 'DART'})
                CREATE (station:Station {
                    name: $StationName,
                    operational: $Operational,
                    location: $Location,
                    address: $StationAddress,
                    eircode: $Eircode,
                    atm: $ATM,
                    weekend_working: $WeekendWorking,
                    wifi: $WiFiAccess,
                    refreshments: $Refreshments,
                    phone_charging: $PhoneCharging,
                    ticket_machine: $TicketVendingMachine,
                    smart_card_enabled: $SmartCardEnabled,
                    routes_serviced: $RoutesServiced
                })
                CREATE (dart)-[:HAS_STATION]->(station)
                """
                self.execute_query(
                    query,
                    parameters={
                        "StationName": row["StationName"],
                        "Operational": row["Operational"],
                        "Location": row["Location"],
                        "StationAddress": row["Station Address"],
                        "Eircode": row["Eircode"],
                        "ATM": row["ATM"],
                        "WeekendWorking": row["Weekend Working"],
                        "WiFiAccess": row["Wi-Fi & Internet Access"],
                        "Refreshments": row["Refreshments"],
                        "PhoneCharging": row["Phone Charging"],
                        "TicketVendingMachine": row["Ticket Vending Machine"],
                        "SmartCardEnabled": row["Smart Card Enabled"],
                        "RoutesServiced": row["Routes Serviced"],
                    },
                )
        print("DART imported successfully!")

    def create_station_relationships(self):
        """
        Create relationships between stations based on shared routes.
        """
        query = """
        MATCH (station1:Station), (station2:Station)
        WHERE station1 <> station2
        AND ANY(route IN split(station1.routes_serviced, ', ') 
                WHERE route IN split(station2.routes_serviced, ', '))
        CREATE (station1)-[:CONNECTED_TO]->(station2)
        """
        self.execute_query(query)
        print("Relationships between stations created successfully!")