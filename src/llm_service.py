import os
import logging
import json
from groq import Groq
from dotenv import load_dotenv
import pandas as pd
import time

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class LLMService:
    """
    LLM service that processes Excel data in batches to answer queries
    """
    
    def __init__(self):
        """Initialize the GROQ client"""
        try:
            self.api_key = os.environ.get("GROQ_API_KEY")
            self.client = Groq(api_key=self.api_key)
            self.model = "llama-3.3-70b-versatile"
            logger.info("LLMService initialized")
            
            # Initialize data storage
            self.hrm_data = None
            self.work_data = None
            self.hrm_shape = None
            self.work_shape = None
            self.common_columns = None
            
            # Batch processing settings
            self.batch_size = 5  # Number of rows per batch
            
        except Exception as e:
            logger.error(f"Error initializing LLM service: {str(e)}")
            self.client = None
    
    def set_data(self, data_package):
        """Store the data for processing"""
        try:
            # Store the dataframes
            self.hrm_data = data_package['hrm_data']
            self.work_data = data_package['work_data']
            
            # Store metadata
            self.hrm_shape = data_package['hrm_shape']
            self.work_shape = data_package['work_shape']
            self.common_columns = data_package['common_columns']
            
            logger.info("Data stored for processing")
        except Exception as e:
            logger.error(f"Error storing data: {str(e)}")
    
    def process_query(self, query):
        """Process a user query using batch processing"""
        try:
            if not self.client or self.hrm_data is None:
                return "Please upload data files first"
            
            # Determine query type for specialized handling
            query_type = self._get_query_type(query)
            
            if query_type == "all_employees":
                return self._list_all_employees()
            elif query_type == "candidate_recommendation":
                return self._process_candidate_recommendation()
            elif query_type == "project_summary":
                return self._process_project_summary(query)
            elif query_type == "work_summary":
                return self._process_work_summary(query)
            else:
                # For general queries, use batch processing
                return self._process_batched_query(query)
            
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            return f"Error processing query: {str(e)}"
    
    def _get_query_type(self, query):
        """Determine the type of query"""
        query_lower = query.lower()
        
        if "all employee" in query_lower or "list all employee" in query_lower or "name of all" in query_lower:
            return "all_employees"
        elif "top candidate" in query_lower or "best candidate" in query_lower or "sales manager role" in query_lower:
            return "candidate_recommendation"
        elif "project summary" in query_lower or "summary of project" in query_lower:
            return "project_summary"
        elif "work contribution" in query_lower or "work summary" in query_lower:
            return "work_summary"
        else:
            return "general"
    
    def _list_all_employees(self):
        """List all employees in the dataset"""
        try:
            # Get employee names and IDs
            if 'employee_name' in self.hrm_data.columns and 'employee_no' in self.hrm_data.columns:
                employees = []
                for i, (_, row) in enumerate(self.hrm_data.iterrows(), 1):
                    name = str(row.get('employee_name', 'Unknown'))
                    emp_no = str(row.get('employee_no', 'Unknown'))
                    employees.append(f"{i}. {name} (ID: {emp_no})")
                
                response = f"Here is the complete list of all {len(self.hrm_data)} employees:\n\n"
                response += "\n".join(employees)
                return response
            else:
                return "Employee name or ID columns not found in the dataset."
        except Exception as e:
            logger.error(f"Error listing employees: {str(e)}")
            return f"Error listing employees: {str(e)}"
    
    def _process_batched_query(self, query):
        """Process a query by analyzing data in batches"""
        try:
            # Step 1: Split the data into batches
            hrm_batches = self._create_batches(self.hrm_data)
            work_batches = self._create_batches(self.work_data)
            
            # Step 2: Process each batch and collect insights
            batch_insights = []
            
            # Process HRM batches
            for i, hrm_batch in enumerate(hrm_batches):
                insight = self._process_hrm_batch(query, hrm_batch, i+1, len(hrm_batches))
                batch_insights.append(insight)
                # Add a small delay to avoid rate limiting
                time.sleep(1)
            
            # Process Work batches
            for i, work_batch in enumerate(work_batches):
                insight = self._process_work_batch(query, work_batch, i+1, len(work_batches))
                batch_insights.append(insight)
                # Add a small delay to avoid rate limiting
                time.sleep(1)
            
            # Step 3: Synthesize the batch insights into a final answer
            final_answer = self._synthesize_insights(query, batch_insights)
            
            return final_answer
            
        except Exception as e:
            logger.error(f"Error in batch processing: {str(e)}")
            return f"Error processing query: {str(e)}"
    
    def _create_batches(self, df):
        """Split a dataframe into batches"""
        batches = []
        total_rows = len(df)
        
        for i in range(0, total_rows, self.batch_size):
            end_idx = min(i + self.batch_size, total_rows)
            batches.append(df.iloc[i:end_idx])
        
        return batches
    
    def _process_hrm_batch(self, query, batch, batch_num, total_batches):
        """Process a batch of HRM data"""
        try:
            # Convert batch to string representation
            batch_data = self._get_compact_data(batch)
            
            # Create a prompt for this batch
            batch_prompt = (
                f"Analyze this batch of HRM data (batch {batch_num}/{total_batches}) to help answer: '{query}'\n\n"
                f"HRM Data Batch:\n{batch_data}\n\n"
                f"Extract key insights from this batch that might help answer the query. "
                f"Focus on facts and patterns, not conclusions yet."
            )
            
            # Call the LLM with a small token count
            response = self.client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": "You are an HR data analyst extracting insights from employee data batches."
                    },
                    {
                        "role": "user",
                        "content": batch_prompt
                    }
                ],
                model=self.model,
                temperature=0.3,
                max_tokens=300
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error processing HRM batch {batch_num}: {str(e)}")
            return f"Error processing HRM batch {batch_num}: {str(e)}"
    
    def _process_work_batch(self, query, batch, batch_num, total_batches):
        """Process a batch of Work data"""
        try:
            # Convert batch to string representation
            batch_data = self._get_compact_data(batch)
            
            # Create a prompt for this batch
            batch_prompt = (
                f"Analyze this batch of Work data (batch {batch_num}/{total_batches}) to help answer: '{query}'\n\n"
                f"Work Data Batch:\n{batch_data}\n\n"
                f"Extract key insights from this batch that might help answer the query. "
                f"Focus on facts and patterns, not conclusions yet."
            )
            
            # Call the LLM with a small token count
            response = self.client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": "You are an HR data analyst extracting insights from work contribution data batches."
                    },
                    {
                        "role": "user",
                        "content": batch_prompt
                    }
                ],
                model=self.model,
                temperature=0.3,
                max_tokens=300
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error processing Work batch {batch_num}: {str(e)}")
            return f"Error processing Work batch {batch_num}: {str(e)}"
    
    def _synthesize_insights(self, query, batch_insights):
        """Synthesize batch insights into a final answer"""
        try:
            # Combine all insights
            combined_insights = "\n\n".join([f"Batch Insight {i+1}:\n{insight}" for i, insight in enumerate(batch_insights)])
            
            # Create a synthesis prompt
            synthesis_prompt = (
                f"Based on the analysis of the entire dataset in batches, answer this HR query: '{query}'\n\n"
                f"Here are the insights extracted from each batch of data:\n\n{combined_insights}\n\n"
                f"Dataset Overview:\n"
                f"- HRM Dataset: {self.hrm_shape[0]} employees, {self.hrm_shape[1]} columns\n"
                f"- Work Dataset: {self.work_shape[0]} entries, {self.work_shape[1]} columns\n"
                f"- Common columns: {self.common_columns}\n\n"
                f"Synthesize these insights into a comprehensive answer to the original query."
            )
            
            # Call the LLM for synthesis
            response = self.client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": "You are an HR assistant that synthesizes data insights to provide comprehensive answers to HR queries."
                    },
                    {
                        "role": "user",
                        "content": synthesis_prompt
                    }
                ],
                model=self.model,
                temperature=0.7,
                max_tokens=800
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error synthesizing insights: {str(e)}")
            return f"Error synthesizing insights: {str(e)}"
    
    def _process_candidate_recommendation(self):
        """Process a candidate recommendation query"""
        try:
            # Extract relevant columns for candidate evaluation
            candidate_data = self.hrm_data.copy()
            
            # Create a prompt for candidate recommendation
            recommendation_prompt = (
                f"Identify and rank the top 5 candidates for a Sales Manager role based on the HRM dataset.\n\n"
                f"Consider these factors:\n"
                f"1. Personality traits suitable for sales management\n"
                f"2. Relevant experience\n"
                f"3. Project involvement\n\n"
                f"HRM Dataset ({self.hrm_shape[0]} employees) - Sample data:\n"
                f"{self._get_compact_data(candidate_data)}\n\n"
                f"Provide a ranked list of the top 5 candidates with justification for each."
            )
            
            # Call the LLM
            response = self.client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": "You are an HR recruitment specialist who identifies the best candidates for specific roles."
                    },
                    {
                        "role": "user",
                        "content": recommendation_prompt
                    }
                ],
                model=self.model,
                temperature=0.7,
                max_tokens=800
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error processing candidate recommendation: {str(e)}")
            return f"Error processing candidate recommendation: {str(e)}"
    
    def _process_project_summary(self, query):
        """Process a project summary query"""
        try:
            # Extract project name from query
            project_keywords = ["workday", "salesforce", "oracle", "sap"]
            query_lower = query.lower()
            target_project = next((p for p in project_keywords if p in query_lower), None)
            
            if not target_project:
                return "Please specify a project name (Workday, Salesforce, Oracle, or SAP)."
            
            # Filter work data for the specified project
            project_work = self.work_data.copy()
            if 'work_summary' in project_work.columns:
                project_work['work_summary'] = project_work['work_summary'].astype(str)
                project_work = project_work[project_work['work_summary'].str.contains(target_project, case=False)]
            
            # Create a prompt for project summary
            summary_prompt = (
                f"Generate a concise summary of key achievements for the {target_project.title()} project.\n\n"
                f"Work entries related to {target_project.title()} project:\n"
                f"{self._get_compact_data(project_work)}\n\n"
                f"Provide a structured summary highlighting:\n"
                f"1. Overall project status\n"
                f"2. Key achievements and milestones\n"
                f"3. Team contributions\n"
            )
            
            # Call the LLM
            response = self.client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": "You are a project management specialist who creates concise project summaries."
                    },
                    {
                        "role": "user",
                        "content": summary_prompt
                    }
                ],
                model=self.model,
                temperature=0.7,
                max_tokens=800
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error processing project summary: {str(e)}")
            return f"Error processing project summary: {str(e)}"
    
    def _process_work_summary(self, query):
        """Process a work summary query for a specific employee"""
        try:
            # Extract employee name from query
            query_words = query.lower().split()
            
            # Find employee in HRM data
            employee_data = self.hrm_data.copy()
            employee_found = False
            employee_id = None
            
            if 'employee_name' in employee_data.columns:
                employee_data['employee_name'] = employee_data['employee_name'].astype(str)
                
                for _, row in employee_data.iterrows():
                    emp_name = str(row['employee_name']).lower()
                    if any(word in emp_name for word in query_words if len(word) > 3):
                        employee_found = True
                        employee_id = row.get('employee_no')
                        employee_name = row.get('employee_name')
                        break
            
            if not employee_found:
                return "Please specify a valid employee name."
            
            # Filter work data for the specified employee
            employee_work = self.work_data.copy()
            if 'employee_no' in employee_work.columns and employee_id is not None:
                employee_work = employee_work[employee_work['employee_no'] == employee_id]
            
            # Create a prompt for work summary
            summary_prompt = (
                f"Extract and display detailed work contributions for {employee_name}.\n\n"
                f"Work entries for this employee:\n"
                f"{self._get_compact_data(employee_work)}\n\n"
                f"Provide a structured summary of this employee's work contributions, including:\n"
                f"1. Key responsibilities\n"
                f"2. Projects involved\n"
                f"3. Notable achievements\n"
            )
            
            # Call the LLM
            response = self.client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": "You are an HR performance analyst who summarizes employee work contributions."
                    },
                    {
                        "role": "user",
                        "content": summary_prompt
                    }
                ],
                model=self.model,
                temperature=0.7,
                max_tokens=800
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error processing work summary: {str(e)}")
            return f"Error processing work summary: {str(e)}"
    
    def _get_compact_data(self, df, num_rows=None):
        """Get a compact string representation of the dataframe"""
        try:
            # Select a subset of rows if specified
            if num_rows is not None:
                sample_df = df.head(num_rows)
            else:
                sample_df = df
            
            # Convert to string representation
            rows = []
            for i, (_, row) in enumerate(sample_df.iterrows(), 1):
                row_str = f"Row {i}: "
                # Add key-value pairs
                for col in sample_df.columns[:10]:  # Limit to first 10 columns
                    value = row.get(col, "")
                    if pd.notna(value):
                        row_str += f"{col}='{str(value)[:30]}', "  # Truncate long values
                rows.append(row_str)
            
            return "\n".join(rows)
        except Exception as e:
            logger.error(f"Error creating compact data: {str(e)}")
            return "Error creating data representation" 