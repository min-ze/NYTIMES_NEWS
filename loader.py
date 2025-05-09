import argparse
import logging
import requests
import time
import logging
from datetime import datetime


log = logging.getLogger(__name__)
BASE_URL = "https://api.nytimes.com/svc/search/v2/articlesearch.json"

def flatten_dict(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, str(v)))
        else:
            items.append((new_key, v))
    return dict(items)

class NYTimesSource(object):
    def __init__(self):
        self.api_key = None
        self.query = None
        self.page = 0
        self.schema = set()

    def connect(self, inc_column=None, max_inc_value=None):
        self.api_key = self.args.api_key
        self.query = self.args.query
        self.page = 0

    def disconnect(self):
        pass
    
    def getDataBatch(self, batch_size, max_items=100):
        batch = []
        total_items_fetched = 0

        while total_items_fetched < max_items:
            params = {
                'q': self.query,
                'api-key': self.api_key,
                'page': self.page
            }

            response = make_request_with_retry(BASE_URL, params=params)

            if response is None or response.status_code != 200:
                log.error(f"Failed to fetch data: {response.status_code if response else 'No response'}")
                break

            docs = response.json().get('response', {}).get('docs', [])
            if not docs:
                break

            for doc in docs:
                if total_items_fetched >= max_items:
                    break 

                flat_doc = flatten_dict(doc)
                self.schema.update(flat_doc.keys())
                batch.append(flat_doc)
                total_items_fetched += 1

                if len(batch) == batch_size:
                    yield batch
                    batch = []

            self.page += 1
            time.sleep(1)

        # Yield the final partial batch (if any)
        if batch:
            yield batch

    def getSchema(self):
        return list(self.schema)


def make_request_with_retry(url, params=None):
    retries = 3
    for _ in range(retries):
        response = requests.get(url, params=params)
        print(f"Response Status Code: {response.status_code}") 
        
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 10)) 
            print(f"Rate limit exceeded, retrying after {retry_after} seconds...")
            time.sleep(retry_after)
        else:
            return response
    return None


if __name__ == "__main__":
    config = {
        "api_key": "LhhIPC3AcjcAJ5cyccQp6tdvUGWsIp1q",  
        "query": "Silicon Valley",
    }

    logging.basicConfig(level=logging.INFO)

    source = NYTimesSource()  
    source.args = argparse.Namespace(**config)
    source.connect()

    for idx, batch in enumerate(source.getDataBatch(batch_size=10, max_items=100)):
        print(f"{idx+1} Batch of {len(batch)} items")

        # Get the schema for this batch
        schema = source.getSchema()
        print("Schema for this batch:")
        print(schema)

        for item in batch:
            # Print only id and headline.main for debugging
            # print(f" - {item.get('_id')} - {item.get('headline.main')}")
            print(item)




