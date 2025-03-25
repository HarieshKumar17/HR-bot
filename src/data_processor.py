import pandas as pd
import logging

logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Simple data processor that loads Excel files
    """
    
    def load_data(self, hrm_file, work_file):
        """
        Load both datasets and prepare them for processing
        """
        try:
            logger.info("Loading Excel datasets")
            
            # Load the datasets
            hrm_data = pd.read_excel(hrm_file)
            work_data = pd.read_excel(work_file)
            
            # Clean the data
            hrm_data = self._clean_dataframe(hrm_data)
            work_data = self._clean_dataframe(work_data)
            
            # Create a data package with metadata
            data_package = {
                "hrm_data": hrm_data,
                "work_data": work_data,
                "hrm_shape": hrm_data.shape,
                "work_shape": work_data.shape,
                "common_columns": list(set(hrm_data.columns).intersection(set(work_data.columns)))
            }
            
            logger.info(f"Data loaded successfully")
            return data_package
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
    
    def _clean_dataframe(self, df):
        """Clean the dataframe for better processing"""
        # Convert column names to strings
        df.columns = df.columns.astype(str)
        
        # Fill NaN values with empty strings for text columns
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].fillna('')
        
        return df 