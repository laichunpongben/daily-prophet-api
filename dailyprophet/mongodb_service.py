import logging

from pymongo import MongoClient

from .configs import MONGODB_USER, MONGODB_PASSWORD, MONGODB_CLUSTER

logger = logging.getLogger(__name__)


class MongoDBService:
    def __init__(self, collection: str):
        self.uri = f"mongodb+srv://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_CLUSTER}/?retryWrites=true&w=majority"
        self.client = MongoClient(self.uri)
        self.db_name = "dailyprophet"
        self.collection_name = collection

    def connect(self):
        try:
            self.client.server_info()  # Check if the connection is successful
            logger.debug("Connected to the database")
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            raise e

    def disconnect(self):
        try:
            self.client.close()
            logger.debug("Disconnected from the database")
        except Exception as e:
            logger.error(f"Error disconnecting from the database: {e}")
            raise e

    def check_id_exists(self, id):
        if not id or not isinstance(id, str):
            raise ValueError(f"Invalid or missing ID provided: {id}")

        database = self.client[self.db_name]
        collection = database[self.collection_name]

        # Check if a document with the specified _id exists
        result = collection.find_one({"_id": id})

        return result is not None

    def read(self, key, key_field="_id"):
        if not key or not isinstance(key, str):
            raise ValueError(f"Invalid or missing key provided: {key}")

        database = self.client[self.db_name]
        collection = database[self.collection_name]

        try:
            result = collection.find_one({key_field: key})

            if not result:
                return {}
            return result
        except Exception as e:
            raise ValueError(f"Invalid ID format: {key}") from e

    def save(self, key, record, key_field="_id"):
        if not key or not isinstance(key, str):
            raise ValueError(f"Invalid or missing key provided: {key}")

        database = self.client[self.db_name]
        collection = database[self.collection_name]

        # Update the score for the document with the specified _id
        result = collection.replace_one({key_field: key}, record, upsert=True)

        return result

    def insert(self, record):
        database = self.client[self.db_name]
        collection = database[self.collection_name]
        collection.insert_one(record)

    def read_all(self):
        database = self.client[self.db_name]
        collection = database[self.collection_name]

        all_records = list(collection.find())
        return all_records

    def sample(self, num_records=2):
        database = self.client[self.db_name]
        collection = database[self.collection_name]

        samples = list(collection.aggregate([{"$sample": {"size": num_records}}]))

        return samples

    def query(self, criteria: dict):
        database = self.client[self.db_name]
        collection = database[self.collection_name]
        return list(collection.find(criteria))


if __name__ == "__main__":
    # db = MongoDBService("readers")

    # new_record = {
    #     "userId": "PUBLIC",
    #     "portfolio": [
    #         ["reddit", "programming", 0.1],
    #         ["arxiv", "cs.LG", 0.1],
    #         ["youtube", "UCqECaJ8Gagnn7YCbPEzWH6g", 0.1],
    #         ["openweathermap", "Hong Kong", 0.02],
    #         ["openweathermap", "Singapore", 0.02],
    #         ["openweathermap", "Dubai", 0.02],
    #     ],
    # }

    # # db.insert(new_record)
    # db.save("PUBLIC", new_record, key_field="userId")

    # # records = db.read_all()
    # # print(records)

    # r1 = db.read("ben", key_field="userId")
    # print(r1)

    # r2 = db.read("PUBLIC", key_field="userId")
    # print(r2)

    from datetime import datetime

    db = MongoDBService("feeds")
    criteria = {
        "source": "reddit",
        "subject": "sex",
        "expire_time": {"$gte": int(datetime.utcnow().timestamp())},
    }
    records = db.query(criteria)
    print(records)
    print(len(records))
