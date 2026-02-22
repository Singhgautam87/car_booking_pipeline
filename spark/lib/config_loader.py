import json
import os

class ConfigLoader:
    """Load and parse configuration from config.json"""
    
    def __init__(self, config_path=None):
        if config_path is None:
            # Try multiple locations
            possible_paths = [
                "/opt/config/config.json",  # Docker container path
                os.path.join(os.path.dirname(__file__), "../../config/config.json"),  # Local relative
                os.path.join(os.getcwd(), "config/config.json"),  # Current working directory
            ]
            
            config_path = None
            for path in possible_paths:
                if os.path.exists(path):
                    config_path = path
                    break
            
            if config_path is None:
                raise FileNotFoundError("config.json not found in any of the expected locations")
        
        with open(config_path, 'r') as f:
            self.config = json.load(f)
    
    def get_minio_config(self):
        """Get MinIO/S3A configuration"""
        return self.config['minio']
    
    def get_kafka_config(self):
        """Get Kafka configuration"""
        return self.config['kafka']
    
    def get_postgres_config(self):
        """Get PostgreSQL configuration"""
        return self.config['postgresql']
    
    def get_mysql_config(self):
        """Get MySQL configuration"""
        return self.config['mysql']
    
    def get_delta_paths(self):
        """Get Delta Lake paths"""
        return self.config['delta_paths']
    
    def get_tables(self):
        """Get table names"""
        return self.config['tables']
    
    def get_postgres_jdbc_url(self):
        """Get PostgreSQL JDBC URL"""
        return self.config['postgresql']['jdbc_url']
    
    def get_mysql_jdbc_url(self):
        """Get MySQL JDBC URL"""
        return self.config['mysql']['jdbc_url']


def load_config(config_path=None):
    """Convenience function to load config"""
    return ConfigLoader(config_path)
