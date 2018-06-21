from modelCursor import ModelCursor


class Wagon(object):
    """ Wagon class.
    """
    db = None
    required_vars = []
    admissible_vars = []

    def __init__(self, **kwargs):
        for i in self.required_vars:
            if i not in kwargs.keys():
                raise ValueError("Required Values not admitted. Object not updated.")

        self.__dict__['goods'] = []
        self.__dict__.update(kwargs)
        self.__dict__['_remainingVolumn'] = self.volumn
        self.__dict__['_remainingWeight'] = self.weight

        if '_id' not in kwargs.keys():
            self.mod_params = list(self.__dict__.keys())
        else:
            self.mod_params = []

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.id == other.id

    def save(self):
        if '_id' not in self.__dict__.keys():
            json = dict()
            for key in self.__dict__.keys():
                if key in self.mod_params:
                    if type(self.__dict__[key]) is list:
                        if key not in json.keys():
                            json[key] = []
                        goods = self.__dict__[key]
                        for position in goods:
                            if 'mod_params' in position.__dict__:
                                good = {k: position.__dict__[k] for k in position.__dict__['mod_params'] if k in position.__dict__}
                                json[key].append(good)
                            else:
                                json[key].append(position)
                    else:
                        json[key] = self.__dict__[key]
            self.db.insert_one(json)
        else:
            data = dict((k, self.__dict__[k]) for k in self.mod_params if k in self.__dict__)
            self.db.update_one({'_id': self.__dict__['_id']}, {'$set': data}, upsert=False)

    def update(self, **kwargs):
        for i in kwargs.keys():
            if i not in self.admissible_vars:
                raise ValueError("Admissible Values not admitted. Object not updated.")

        self.__dict__.update(kwargs)
        if ['_id' in self.__dict__.keys()]:
            for key in kwargs.keys():
                if key not in self.mod_params:
                    self.mod_params.append(key)

    def assign_good(self, good):
        # if mercancia in self.mercancias:
        #     raise ValueError("Mercancia already in Vagon.")

        if self._remainingVolume > good.volumn:
            if self._remainingWeight > good.weight:
                self._remainingVolume -= good.volumn
                self._remainingWeight -= good.weight
                self.mercancias.append(good)
                self.update(mercancias=self.__dict__['goods'])
            else:
                print("Weight not admitted. Mercancia not added.")
                #raise ValueError("Weight not admitted. Mercancia not added.")
        else:
            print("Volume not admitted. Mercancia not added.")
            #raise ValueError("Volume not admitted. Mercancia not added.")

    def unassign_good(self, good):
        if good in self.goods:
            self._remainingWeight += good.weight
            self._remainingVolume += good.volumn
            self.goods.remove(good)
            self.update(mercancias=self.__dict__['goods'])
            self.save()
        else:
            raise ValueError("Good not found.")

    @classmethod
    def query(cls, query):
        cursor = cls.db.aggregate(query)
        cursor = ModelCursor(cls, cursor)
        return cursor

    @classmethod
    def init_class(cls, db, vars_path="model_name.vars"):
        with open(vars_path, 'r') as vars_file:
            cls.required_vars = vars_file.readline().split()
            cls.admissible_vars = vars_file.readline().split()
        cls.db = db
