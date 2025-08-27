#!/usr/bin/env python3
"""
Neo4j Data Ingest Script

This script loads CRM data from CSV files into Neo4j using the data model
and configuration specified in ingest_config.yaml and data_model/data_model.json.

Usage:
    python ingest.py

Requirements:
    - Neo4j database running and accessible
    - .env file with database credentials
    - CSV files in data/ directory
    - ingest_config.yaml configuration file
"""

import os
import csv
import json
import yaml
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

from neo4j import GraphDatabase
from dotenv import load_dotenv


class Neo4jIngest:
    """Main class for ingesting CRM data into Neo4j."""
    
    def __init__(self, config_file: str = "ingest_config.yaml"):
        """Initialize the ingest process."""
        self.setup_logging()
        self.load_environment()
        self.load_config(config_file)
        
        self.connect_to_neo4j()
        
    def setup_logging(self):
        """Configure logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('ingest.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def load_environment(self):
        """Load environment variables from .env file."""
        load_dotenv()
        self.neo4j_uri = os.getenv("NEO4J_URI")
        self.neo4j_username = os.getenv("NEO4J_USERNAME")
        self.neo4j_password = os.getenv("NEO4J_PASSWORD")
        self.neo4j_database = os.getenv("NEO4J_DATABASE", "neo4j")
        
        if not all([self.neo4j_uri, self.neo4j_username, self.neo4j_password]):
            raise ValueError("Missing required Neo4j credentials in .env file")
            
    def load_config(self, config_file: str):
        """Load YAML configuration file."""
        try:
            with open(config_file, 'r') as f:
                self.config = yaml.safe_load(f)
                self.logger.info(f"Loaded configuration from {config_file}")
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file {config_file} not found")
            
    def connect_to_neo4j(self):
        """Establish connection to Neo4j database."""
        try:
            self.driver = GraphDatabase.driver(
                self.neo4j_uri,
                auth=(self.neo4j_username, self.neo4j_password)
            )
            # Test connection
            self.driver.verify_connectivity()
            self.logger.info("Successfully connected to Neo4j")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Neo4j: {e}")
            
    def run_query(self, query: str, parameters: Optional[Dict] = None):
        """Execute a Cypher query."""
        with self.driver.session(database=self.neo4j_database) as session:
            try:
                result = session.run(query, parameters or {})
                return result.consume()
            except Exception as e:
                self.logger.error(f"Query failed: {query}")
                self.logger.error(f"Error: {e}")
                raise
                
    def create_constraints(self):
        """Create database constraints."""
        self.logger.info("Creating database constraints...")
        constraints = self.config.get('initializing_queries', {}).get('constraints', [])
        
        for constraint in constraints:
            try:
                self.run_query(constraint)
                self.logger.info(f"Created constraint: {constraint}")
            except Exception as e:
                if "already exists" in str(e).lower():
                    self.logger.info(f"Constraint already exists: {constraint}")
                else:
                    self.logger.error(f"Failed to create constraint: {e}")
                    raise
                    
    def create_indexes(self):
        """Create database indexes."""
        self.logger.info("Creating database indexes...")
        indexes = self.config.get('initializing_queries', {}).get('indexes', []) or []
        
        if not indexes:
            self.logger.info("No indexes to create")
            return
            
        for index in indexes:
            try:
                self.run_query(index)
                self.logger.info(f"Created index: {index}")
            except Exception as e:
                if "already exists" in str(e).lower():
                    self.logger.info(f"Index already exists: {index}")
                else:
                    self.logger.error(f"Failed to create index: {e}")
                    raise
                    
    def load_csv_data(self, file_path: str, field_mappings: Dict[str, str]) -> List[Dict]:
        """Load and transform CSV data according to field mappings."""
        records = []
        csv_path = Path("data") / file_path
        
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
            
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                record = {}
                for target_field, source_field in field_mappings.items():
                    value = row.get(source_field, '')
                    
                    # Handle empty values
                    if value == '' or value is None:
                        record[target_field] = None
                    else:
                        # Type conversion based on field names
                        record[target_field] = self.convert_field_value(target_field, value)
                        
                records.append(record)
                
        self.logger.info(f"Loaded {len(records)} records from {csv_path}")
        return records
        
    def convert_field_value(self, field_name: str, value: str) -> Any:
        """Convert field values to appropriate types."""
        if value is None or value == '':
            return None
            
        # Convert numeric fields
        if any(keyword in field_name.lower() for keyword in ['revenue', 'amount', 'number', 'probability']):
            try:
                return int(value) if value.isdigit() else int(float(value))
            except (ValueError, TypeError):
                return None
                
        # Convert date fields
        if any(keyword in field_name.lower() for keyword in ['date', 'created', 'closed']):
            if value and value.strip():
                try:
                    # Try common date formats
                    for date_format in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']:
                        try:
                            return datetime.strptime(value, date_format).strftime('%Y-%m-%d')
                        except ValueError:
                            continue
                    return value  # Return original if no format matches
                except:
                    return None
            return None
            
        return str(value)
        
    def generate_case_owner_id(self, name: str) -> str:
        """Generate a unique ID for case owners from their names."""
        if not name or name.strip() == '':
            return None
        return name.lower().replace(' ', '_').replace('.', '')
        
    def load_case_owners(self):
        """Load unique case owners from cases.csv."""
        self.logger.info("Loading case owners...")
        
        # Get unique case owners from cases.csv
        csv_path = Path("data") / "cases.csv"
        case_owners = set()
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                owner = row.get('Case_Owner', '').strip()
                if owner:
                    case_owners.add(owner)
        
        # Create records for case owners
        records = []
        for owner_name in case_owners:
            records.append({
                'ownerId': self.generate_case_owner_id(owner_name),
                'name': owner_name
            })
            
        # Load into Neo4j
        query = self.config['loading_queries']['nodes']['CaseOwner']['query']
        self.run_query(query, {'records': records})
        self.logger.info(f"Loaded {len(records)} case owners")
        
    def load_nodes(self):
        """Load all node types."""
        self.logger.info("Loading nodes...")
        
        # Load case owners first (they're derived from other data)
        self.load_case_owners()
        
        # Load other nodes
        nodes_config = self.config.get('loading_queries', {}).get('nodes', {})
        
        for node_type, config in nodes_config.items():
            if node_type == 'CaseOwner':
                continue  # Already loaded above
                
            self.logger.info(f"Loading {node_type} nodes...")
            
            source_file = config['source_file']
            field_mappings = config['field_mappings']
            query = config['query']
            
            records = self.load_csv_data(source_file, field_mappings)
            
            # Process records in batches
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                self.run_query(query, {'records': batch})
                
            self.logger.info(f"Loaded {len(records)} {node_type} nodes")
            
    def load_relationships(self):
        """Load all relationships."""
        self.logger.info("Loading relationships...")
        
        relationships_config = self.config.get('loading_queries', {}).get('relationships', {})
        
        for relationship_type, config in relationships_config.items():
            if relationship_type == 'CONVERTED_TO_OPPORTUNITY':
                self.logger.info(f"Skipping {relationship_type} - requires custom mapping logic")
                continue
                
            self.logger.info(f"Loading {relationship_type} relationships...")
            
            source_data = config['source_data']
            field_mappings = config['field_mappings']
            query = config['query']
            
            # Special handling for ASSIGNED_TO relationship
            if relationship_type == 'ASSIGNED_TO':
                records = self.load_assigned_to_relationships()
            else:
                records = self.load_csv_data(source_data, field_mappings)
            
            # Process records in batches
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                try:
                    self.run_query(query, {'records': batch})
                except Exception as e:
                    self.logger.error(f"Failed to load {relationship_type}: {e}")
                    continue
                    
            self.logger.info(f"Loaded {len(records)} {relationship_type} relationships")
            
    def load_assigned_to_relationships(self) -> List[Dict]:
        """Load ASSIGNED_TO relationships with proper case owner ID transformation."""
        records = []
        csv_path = Path("data") / "cases.csv"
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                case_id = row.get('Case_ID', '').strip()
                case_owner = row.get('Case_Owner', '').strip()
                
                if case_id and case_owner:
                    records.append({
                        'sourceId': case_id,
                        'targetId': self.generate_case_owner_id(case_owner)
                    })
                    
        return records
        
    def run_ingest(self):
        """Run the complete data ingest process."""
        try:
            self.logger.info("Starting Neo4j data ingest...")
            
            # Step 1: Create constraints
            self.create_constraints()
            
            # Step 2: Create indexes (if any)
            self.create_indexes()
            
            # Step 3: Load nodes
            self.load_nodes()
            
            # Step 4: Load relationships
            self.load_relationships()
            
            self.logger.info("Data ingest completed successfully!")
            
        except Exception as e:
            self.logger.error(f"Data ingest failed: {e}")
            raise
            
    def verify_data(self):
        """Verify the loaded data by running some basic queries."""
        self.logger.info("Verifying loaded data...")
        
        verification_queries = [
            ("Total Accounts", "MATCH (n:Account) RETURN count(n) as count"),
            ("Total Contacts", "MATCH (n:Contact) RETURN count(n) as count"),
            ("Total Cases", "MATCH (n:Case) RETURN count(n) as count"),
            ("Total Opportunities", "MATCH (n:Opportunity) RETURN count(n) as count"),
            ("Total Leads", "MATCH (n:Lead) RETURN count(n) as count"),
            ("Total Case Owners", "MATCH (n:CaseOwner) RETURN count(n) as count"),
            ("Account-Contact relationships", "MATCH (:Contact)-[:BELONGS_TO_ACCOUNT]->(:Account) RETURN count(*) as count"),
            ("Account-Case relationships", "MATCH (:Account)-[:HAS_CASE]->(:Case) RETURN count(*) as count"),
            ("Case-Contact relationships", "MATCH (:Case)-[:REPORTED_BY]->(:Contact) RETURN count(*) as count"),
            ("Account-Opportunity relationships", "MATCH (:Account)-[:HAS_OPPORTUNITY]->(:Opportunity) RETURN count(*) as count"),
            ("Case-Owner relationships", "MATCH (:Case)-[:ASSIGNED_TO]->(:CaseOwner) RETURN count(*) as count"),
        ]
        
        with self.driver.session(database=self.neo4j_database) as session:
            for description, query in verification_queries:
                try:
                    result = session.run(query)
                    count = result.single()["count"]
                    self.logger.info(f"{description}: {count}")
                except Exception as e:
                    self.logger.error(f"Verification query failed for {description}: {e}")


def main():
    """Main entry point."""
    ingest = None
    try:
        ingest = Neo4jIngest()
        ingest.run_ingest()
        ingest.verify_data()
        
    except Exception as e:
        logging.error(f"Ingest process failed: {e}")
        return 1
    finally:
        if ingest and ingest.driver:
            ingest.driver.close()
        
    return 0


if __name__ == "__main__":
    exit(main())