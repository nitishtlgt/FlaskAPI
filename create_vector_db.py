import pandas as pd
from typing import Optional, Union, List, Tuple
import chromadb 
from chromadb.config import Settings
import json

class CreateCollection:
    """Class to create and manage a collection in a chromadb database."""
    
    def __init__(self, collection_name: str, db_path: Optional[str] = None) -> None:
        """
        Initialize the CreateCollection class.

        Args:
            collection_name (str): The name of the collection to be created or managed.
            db_path (Optional[str]): The path to the database. Defaults to './db' if None.
        """
        self.collection_name = collection_name
        self.db_path = db_path if db_path else './db'
        self.EXISTING_DB = False

    def _create_client(self):
        """Create a chromadb client."""
        return chromadb.PersistentClient(path=self.db_path, settings=Settings(allow_reset=True))

    def create_collection(self):
        """Create a new collection in the database."""
        client = self._create_client()
        try:
            collection = client.get_collection(name=self.collection_name)
            print("Database exists.")
            self.EXISTING_DB = True
        except:
            print('Creating database...')
            client.reset()
            collection = client.create_collection(self.collection_name,
                                                  metadata={"hnsw:space": "cosine"})
            self.EXISTING_DB = False
            
        return collection

    def fill_collection_csv(self, csv_path: str):
        """Fill the collection with data from a CSV file."""
        df = pd.read_csv(csv_path)
        sentences = df['Info'].str.split('.').tolist()

        documents = [sentence[0] for sentence in sentences]
        ids = [str(index) for index, _ in enumerate(sentences)]

        db_collection = self.create_collection()

        if not self.EXISTING_DB:
            db_collection.add(documents=documents,
                                    metadatas=None,
                                    ids=ids)
        return db_collection
            
    def run_query(self,collection, query: str) -> List[Tuple]:
        """Run a query against the collection."""
        results = collection.query(query_texts=[query], n_results=3)

        return results
    
    # Which collection to select If fill _collection =True then we need csv path so we can fill the csv
    def db_collection(self, fill_collection:bool, csv_path:Optional[str] = None):
        collection_manager = CreateCollection('real_estate_test')
        self.fill_collection = fill_collection
        if fill_collection:
            # db_collection = collection_manager.create_collection()
            db_collection = collection_manager.fill_collection_csv(csv_path)  # If we want to add csv data to database
        else:
            db_collection = collection_manager.create_collection()
        
        return db_collection


if __name__ == "__main__":
    csv_path = 'vector_DB.csv'
    collection_manager = CreateCollection('real_estate_test')
    db_collection = collection_manager.db_collection(True,csv_path)
    query_result = collection_manager.run_query(db_collection,'I want furnished house with 1 bed and 1 bath in sarjah area around 40,000AED')
    json_format = json.dumps(query_result)
    
