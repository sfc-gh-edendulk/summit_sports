"""
Base class for data generators in the Summit Sports data generation project.
"""

import os
import yaml
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import pandas as pd
from faker import Faker


class BaseDataGenerator(ABC):
    """
    Abstract base class for all data generators.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the base data generator.
        
        Args:
            config_path: Path to configuration file. If None, uses default config.
        """
        self.config = self._load_config(config_path)
        self.fake = Faker()
        self.fake.seed_instance(self.config.get('generation', {}).get('random_seed', 42))
        
    def _load_config(self, config_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Configuration dictionary
        """
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), 
                'config', 
                'config.yaml'
            )
        
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            
        # Replace environment variables
        return self._replace_env_vars(config)
    
    def _replace_env_vars(self, obj):
        """
        Recursively replace environment variables in configuration.
        """
        if isinstance(obj, dict):
            return {k: self._replace_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._replace_env_vars(item) for item in obj]
        elif isinstance(obj, str) and obj.startswith('${') and obj.endswith('}'):
            env_var = obj[2:-1]
            return os.getenv(env_var, obj)
        else:
            return obj
    
    @abstractmethod
    def generate_data(self, num_records: int) -> pd.DataFrame:
        """
        Generate data for this entity type.
        
        Args:
            num_records: Number of records to generate
            
        Returns:
            DataFrame containing generated data
        """
        pass
    
    def save_data(self, data: pd.DataFrame, filename: str, output_dir: Optional[str] = None) -> str:
        """
        Save generated data to file.
        
        Args:
            data: DataFrame to save
            filename: Name of output file (without extension)
            output_dir: Output directory (defaults to config data directory)
            
        Returns:
            Path to saved file
        """
        if output_dir is None:
            output_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), 
                'data'
            )
        
        os.makedirs(output_dir, exist_ok=True)
        
        output_format = self.config.get('output', {}).get('format', 'csv')
        include_headers = self.config.get('output', {}).get('include_headers', True)
        
        if output_format == 'csv':
            filepath = os.path.join(output_dir, f"{filename}.csv")
            data.to_csv(filepath, index=False, header=include_headers)
        elif output_format == 'json':
            filepath = os.path.join(output_dir, f"{filename}.json")
            data.to_json(filepath, orient='records', indent=2)
        elif output_format == 'parquet':
            filepath = os.path.join(output_dir, f"{filename}.parquet")
            data.to_parquet(filepath, index=False)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
        
        return filepath
    
    def get_config_value(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key_path: Dot-separated path to configuration value (e.g., 'generation.customers')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key_path.split('.')
        value = self.config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
                
        return value
