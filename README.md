# New York Times News Fetcher (Python)
This project uses the New York Times API to fetch articles based on a search query and returns them in a flattened Python dictionary format for easier data handling and analysis.

## Features
- Connects to the New York Times Article Search API
- Fetches news articles in batches
- Flattens nested JSON structures into flat Python dictionaries
- Handles API rate limits using automatic retry logic
- Dynamically generates and updates the schema from fetched data

## Requirements
- Listed in the requirements.txt
Install dependencies with: 
pip install -r requirements.txt

## Assumptions
- Dynamic schema is for a batch of articles instead of just 1 article. 

## Additional Considerations
- Faced error 429 several times (too many requests) and implemented a function to handle API rate limits using automatic retry.
- Retry time is set at 10 seconds for now (works better at 60 but for the sake of running the code, reduced to 10 seconds).
- This document is refined with the help of helper tools.
