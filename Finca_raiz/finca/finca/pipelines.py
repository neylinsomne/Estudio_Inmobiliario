import pymongo

class MongoDBPipeline(object):

    def __init__(self) -> None:
        # Initialize the pipeline and connect to the MongoDB client
        self.client = pymongo.MongoClient("mongodb+srv://insomnesoul:hHR4c1WmJO37GlEA@realstatecolombia.hhtn5cb.mongodb.net/")
        self.collection = self.client['Finca_inmuebles']['Scrapeo_semanal'] # Access the collection through the amazon database
        self.collection_proyes = self.client['Finca_inmuebles']['proy_finca']
        self.collection_indi= self.client['Finca_inmuebles']['indi_finca']

    def close_spider(self, spider):
        # This will end DB connection when spider is interrupted or shutsdown
        self.client.close()

#    def process_item(self, item, spider):
        # Insert the data to the collection and return message about the number of data added to the collection
#        self.collection.insert_one(dict(item))
#        return f"Total number of products scraped: {self.collection.count_documents({})}"
    
    def process_item(self, item, spider):
        
        if isinstance(item,dict):
            self.collection.insert_one(dict(item))
            self.collection_indi.insert_one(dict(item))     
        # Check if there are variations
        elif isinstance(item,list):
            # Insert each variation to the collection
            for variation in item:
                self.collection.insert_one(variation)
                self.collection_proyes.insert_one(variation)
    
            return f"Total number of products scraped: {self.collection.count_documents({})} and Total number of projects scraped: {self.collection_proyes.count_documents({})}"
        else:
            return f"Total number of products scraped: {self.collection.count_documents({})} and Total number of projects scraped: {self.collection_indi.count_documents({})}"
        
        