from pymongo import GEOSPHERE


def get_city_geo_json(city_name):
    """ It returns coordinates of a City by his name
    Parameters:
        city_name (str) -- City's name
    Return:
        (Point) -- (longitude, latitude)
    """
    from geopy.geocoders import Nominatim
    from geojson import Point
    geolocator = Nominatim()

    location = geolocator.geocode(city_name, timeout=200)

    return Point((location.longitude, location.latitude))


class Ware(object):
    """ Ware class
    """
    db              = None
    required_vars   = []
    admissible_vars = []

    def __init__(self, **kwargs):
        if '_id' not in kwargs.keys():
            for i in self.required_vars:
                if i not in kwargs.keys():
                    raise ValueError("Required Values not admitted. Object not updated.")

        self.__dict__.update(kwargs)

        if type(self.date) is str:
            self.date = datetime.datetime.strptime(self.date, "%d/%m/%Y")

        self.__dict__['geolocOrigin']      = get_city_geo_json(self.origin)
        self.__dict__['geolocDestination'] = get_city_geo_json(self.destination)

        if '_id' not in kwargs.keys():
            self.mod_params = list(self.__dict__.keys())
        else:
            self.mod_params = []

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def save(self):
        # Save updates our data in the database, it is our only way to communicate with it
        if '_id' not in self.__dict__.keys():
            self.db.insert_one(dict((key, self.__dict__[key]) for key in self.mod_params if key in self.__dict__))
        else:
            self.db.update_one({'_id': self.__dict__['_id']},
                               {'$set': dict((k, self.__dict__[k]) for k in self.mod_params if k in self.__dict__)},
                               upsert=False)

    def update(self, **kwargs):
        for i in kwargs.keys():
            if i not in self.admissible_vars:
                raise ValueError("Admissible Values not admitted. Object not updated.")

        self.__dict__.update(kwargs)
        if ['_id' in self.__dict__.keys()]:
            for key in kwargs.keys():
                self.mod_params.append(key)

    @classmethod
    def query(cls, query):
        """ Returns a cursor of models
        """
        cursor = cls.db.aggregate(query)
        cursor = ModelCursor(cls, cursor)
        return cursor

    @classmethod
    def init_class(cls, db, vars_path="model_name.vars"):
        """ Initialize class variables
        Paremeters:
            db (MongoClient) -- Conection with MongoDB
            vars_path (str) -- path to file with variables definition
        """
        with open(vars_path, 'r') as vars_file:
            cls.required_vars = vars_file.readline().split()
            cls.admissible_vars = vars_file.readline().split()
        cls.db = db
        cls.db.create_index([("geolocOrigin", GEOSPHERE), ("geolocDestination", GEOSPHERE)])
