import json
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['restaurants_db']
collection = db['restaurants']

json_file_path = 'C:/Users/bagon/PycharmProjects/2_uzduotis/retaurants.json'

with open(json_file_path, 'r', encoding='utf-8') as file:
    for line in file:
        data = json.loads(line)
        collection.insert_one(data)

def get_all_documents():
    return list(collection.find())

def count_documents():
    return collection.count_documents({})

def get_selected_fields():
    return list(collection.find({}, {'restaurant_id': 1, 'name': 1, 'borough': 1, 'cuisine': 1}))

def get_selected_fields_exclude_id():
    return list(collection.find({}, {'restaurant_id': 1, 'name': 1, 'borough': 1, 'cuisine': 1, '_id': 0}))

def get_bronx_restaurants():
    return list(collection.find({'borough': 'Bronx'}))

def get_restaurants_with_score_between_80_and_100():
    pipeline = [
        {
            '$unwind': '$grades'
        },
        {
            '$group': {
                '_id': '$_id',
                'name': {'$first': '$name'},
                'borough': {'$first': '$borough'},
                'cuisine': {'$first': '$cuisine'},
                'grades_score': {'$push': '$grades.score'}
            }
        },
        {
            '$match': {
                'grades_score': {'$gte': 80, '$lte': 100}
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def get_restaurants_sorted():
    return list(collection.find().sort([('cuisine', 1), ('borough', -1)]))

def clear_collection():
    collection.delete_many({})

if __name__ == '__main__':
    print("Visi dokumentai:")
    print(get_all_documents())

    print("Dokumentų skaičius kolekcijoje:")
    print(count_documents())

    print("\nPasirinkti laukai:")
    print(get_selected_fields())

    print("\nPasirinkti laukai be _id:")
    print(get_selected_fields_exclude_id())

    print("\nBronx restoranai:")
    print(get_bronx_restaurants())

    print("\nRestoranai su įvertinimu tarp 80 ir 100")
    print(get_restaurants_with_score_between_80_and_100())

    print("\nRestoranai išsortinti")
    print(get_restaurants_sorted())

    clear_collection()
    print("\nKolekcija išvalyta")
    print("Dokumentų skaičius kolekcijoje po išvalymo:")
    print(count_documents())