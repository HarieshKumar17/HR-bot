# HR Data Assistant

HR Data Assistant is a Streamlit-based web application that allows you to upload HR and work summary Excel files and ask natural language questions about the data. The app uses a custom LLM service (powered by GROQ) to process your queries and return insights by analyzing the datasets in batches.

## Features

- **Data Upload:** Upload HRM Master and Work Summary Excel files via the sidebar.
- **Data Processing:** Data is cleaned, combined, and stored with metadata.
- **LLM Query Processing:** Process user queries in batches (HRM and Work data) for efficient analysis.
- **Chat Interface:** Interact with the app using a chat-like interface to ask HR-related questions.
- **Insights Synthesis:** Combines batch insights to produce comprehensive answers.

## Requirements

- Python 3.7+
- Streamlit
- pandas
- openpyxl
- groq
- python-dotenv
- scikit-learn
- pyngrok (if you need to expose your local app)
- npm (for localtunnel, if desired)

## Installation

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/HarieshKumar17/HR-bot.git
   cd HR-bot
   ```

2. **Install Python Dependencies:**

   Install the required Python packages using pip (you can also use the provided `requirements.txt`):

   ```bash
   pip install -r requirements.txt
   ```

3. **Set Up Environment Variables:**

   Create a `.env` file in the root directory with the following variables (adjust values as needed):

   ```env
   GROQ_API_KEY=your_groq_api_key_here
   LOG_LEVEL=INFO
   ```

## Running the Application

### Locally

1. **Run the Streamlit App:**

   In the project root (where `app.py` is located), execute:

   ```bash
   streamlit run app.py
   ```

2. **Access the Application:**

   Open the provided local URL (usually [http://localhost:8501](http://localhost:8501)) in your browser.

### In Google Colab

If you are running the app in Google Colab, you might use localtunnel or ngrok to expose your app:

Below is a simple code snippet that uses localtunnel to expose your Streamlit app running on Colab. Copy and run this code in a Colab cell:

```bash
!npm install -g localtunnel
!pkill -f streamlit || true
!streamlit run app.py &>/dev/null&
!curl -s https://loca.lt/mytunnelpassword
!lt --port 8501
```

### What Each Command Does

1. **Install localtunnel:**  
   `!npm install -g localtunnel` installs localtunnel globally via npm.

2. **Kill Existing Streamlit Processes:**  
   `!pkill -f streamlit || true` stops any running instances of Streamlit to avoid conflicts.

3. **Run the Streamlit App:**  
   `!streamlit run app.py &>/dev/null&` starts your Streamlit app in the background and suppresses output.

4. **Retrieve Tunnel Password:**  
   `!curl -s https://loca.lt/mytunnelpassword` fetches your tunnel password (which is typically your public IP) that you'll need when prompted.

5. **Expose the App with localtunnel:**  
   `!lt --port 8501` creates a tunnel to port 8501, providing you with a public URL to access your app.

### How to Use This Tunnel

1. **Run the Code:**  
   Execute the above commands in a Colab cell.

2. **Get the Public URL:**  
   After running, localtunnel will output a URL like `https://your-tunnel-name.loca.lt`. Open this URL in your browser.

3. **Enter Tunnel Password (If Prompted):**  
   When accessing the URL, if a password prompt appears, use the output from the `curl` command as the tunnel password.

This setup should allow you to access your Streamlit app running on Colab from any browser.

## Project Structure

- **app.py:** Main Streamlit application that loads data, initializes services, and provides the chat interface.
- **src/**
  - **data_processor.py:** Contains the `DataProcessor` class to load and clean Excel files.
  - **llm_service.py:** Contains the `LLMService` class that processes queries using batch processing.
- **logs/:** Directory for application logs.
- **.env:** File to store environment variables.

## Troubleshooting

- **Data Not Loading:** Ensure that your Excel files have the correct column names (e.g., `employee_name`, `employee_no`).
- **LLM Service Errors:** Verify your GROQ API key and that your API is accessible.
- **Streamlit Tunneling Issues:** Double-check your tunnel setup and confirm that the tunnel password (public IP) is entered correctly when prompted.
- **Check Logs:** Refer to the `logs/app.log` file for detailed error messages.

## License

Specify your license here (for example, MIT License).

## Acknowledgements

- Powered by [GROQ](https://www.groq.com) and Streamlit.
- Inspired by HR analytics and data-driven decision making.

---

