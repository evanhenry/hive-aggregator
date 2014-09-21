
class Dump:
    def _init__(self):
        self.MONGO_ADDR = "127.0.0.1"
        self.MONGO_PORT = 27017
        self.MONGO_DB = "HiveAggregator"
        self.mongo_client = MongoClient(self.MONGO_ADDR, self.MONGO_PORT)
        self.mongo_db = self.mongo_client[self.MONGO_DB]
        
    def query_samples(self, hours):
        print('[Querying Samples in Range] %s' % datetime.strftime(datetime.now(), self.TIME_FORMAT))
        print('\tRange: ' + str(hours))
        time_range = datetime.now() - timedelta(hours = hours) # get datetime
        with open(self.DATA_PATH + 'samples.json', 'w') as jsonfile:
            result = []
            for name in self.mongo_db.collection_names():
                if not name == 'system.indexes':
                    # Add filters here to only include average values
                    for sample in self.mongo_db[name].find({'type':'sample', 'time':{'$gt': time_range, '$lt':datetime.now()}}):
                        print sample
                        sample['time'] = datetime.strftime(sample['time'], self.TIME_FORMAT)
                        result.append(sample)
            dump = json_util.dumps(result, indent=4)
            jsonfile.write(dump)

if __name__ == '__main__':
    root = Dump()
    root.query_samples(168)
