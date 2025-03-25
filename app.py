import os
import logging
import streamlit as st
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import custom modules
from src.data_processor import DataProcessor
from src.llm_service import LLMService

# Configure logging
log_level = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "data_package" not in st.session_state:
    st.session_state.data_package = None

# Page configuration
st.set_page_config(page_title="HR Data Assistant", page_icon="👨‍💼")

# Application title
st.title("HR Data Assistant")

# Initialize services
data_processor = DataProcessor()
llm_service = LLMService()

# Sidebar for data upload
with st.sidebar:
    st.header("Data Upload")
    
    hrm_file = st.file_uploader("Upload HRM Master Dataset", type=["xlsx", "xls"])
    work_file = st.file_uploader("Upload Work Summary Dataset", type=["xlsx", "xls"])
    
    if hrm_file and work_file:
        try:
            # Process uploaded files
            with st.spinner("Loading data..."):
                st.session_state.data_package = data_processor.load_data(hrm_file, work_file)
                llm_service.set_data(st.session_state.data_package)
            
            st.success("Data loaded successfully!")
            
            # Display data statistics
            st.write(f"HRM Dataset: {st.session_state.data_package['hrm_shape'][0]} records")
            st.write(f"Work Dataset: {st.session_state.data_package['work_shape'][0]} records")
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            st.error(f"Error loading data: {str(e)}")

# Main chat interface
st.subheader("Ask questions about your HR data")

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.write(message["content"])

# Get user input
if prompt := st.chat_input("Ask me anything about the HR data..."):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Display user message
    with st.chat_message("user"):
        st.write(prompt)
    
    # Check if data is loaded
    if st.session_state.data_package is None:
        with st.chat_message("assistant"):
            st.write("Please upload both Excel files to continue.")
            st.session_state.messages.append({"role": "assistant", "content": "Please upload both Excel files to continue."})
    else:
        # Process the query
        with st.chat_message("assistant"):
            with st.spinner("Analyzing data..."):
                try:
                    # Process query with LLM
                    response = llm_service.process_query(prompt)
                    
                    # Display response
                    st.write(response)
                    st.session_state.messages.append({"role": "assistant", "content": response})
                    
                except Exception as e:
                    logger.error(f"An error occurred: {str(e)}")
                    st.error(f"An error occurred: {str(e)}")
                    st.session_state.messages.append({"role": "assistant", "content": f"An error occurred: {str(e)}"})

# Footer
st.caption("HR Data Assistant | Powered by GROQ") 