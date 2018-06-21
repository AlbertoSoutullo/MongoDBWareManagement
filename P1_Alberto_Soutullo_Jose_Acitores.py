__author__ = 'Alberto Soutullo Jose Acitores'

from pymongo import MongoClient
from pymongo import GEOSPHERE
import datetime


def getCityGeoJSON(city_name):
    """ Devuelve las coordenadas de una ciudad a partir de su nombre
    Argumentos:
        city_name (str) -- Nombre de la ciudad
    Return:
        (str) -- GeoJSON
    """
    from geopy.geocoders import Nominatim
    from geojson import Point
    geolocator = Nominatim()

    location = geolocator.geocode(city_name, timeout=200)
    #TODO
    # Devolver GeoJSON de tipo punto con la latitud y longitud almacenadas
    # en las variables location.latitude y location.longitude

    return Point((location.longitude, location.latitude))


class ModelCursor(object):
    """ Cursor para iterar sobre los documentos del resultado de una
    consulta. Los documentos deben ser devueltos en forma de objetos
    modelo.
    """

    def __init__(self, model_class, command_cursor):
        """ Inicializa ModelCursor
        Argumentos:
            model_class (class) -- Clase para crear los modelos del
            documento que se itera.
            command_cursor (CommandCursor) -- Cursor de pymongo
        """
        # TODO
        self.__command_cursor__ = command_cursor
        self.model_class = model_class

    def next(self):
        """ Devuelve el siguiente documento en forma de modelo
        """
        # TODO
        document = self.__command_cursor__.next()
        document = self.model_class(**document)
        return document

    @property
    def alive(self):
        """True si existen más modelos por devolver, False en caso contrario
        """
        # TODO
        return self.__command_cursor__.alive


class Mercancia(object):
    """ Prototipo de la clase modelo
        Copiar y pegar tantas veces como modelos se deseen crear (cambiando
        el nombre Model, por la entidad correspondiente), o bien crear tantas
        clases como modelos se deseen que hereden de esta clase. Este segundo
        metodo puede resultar mas compleja
    """
    required_vars = []
    admissible_vars = []
    db = None

    def __init__(self, **kwargs):
        # **kwargs es un parámetro generico para diccionarios.
        # Mirar si el campo id está, si no está es que es instanciado y no se guarda en las actualizaciones

        if '_id' not in kwargs.keys():
            for i in self.required_vars:
                if i not in kwargs.keys():
                    raise ValueError("Required Values not admitted. Object not updated.")

        self.__dict__.update(kwargs)
        if type(self.fecha) is str:
            self.fecha = datetime.datetime.strptime(self.fecha, "%d/%m/%Y")
        self.__dict__['geolocOrigen'] = getCityGeoJSON(self.origen)
        self.__dict__['geolocDestino'] = getCityGeoJSON(self.destino)

        if '_id' not in kwargs.keys():
            self.mod_params = list(self.__dict__.keys())
        else:
            self.mod_params = []

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def save(self):
        # imprimirlo por pantalla como json
        # Actualización de los datos en la base de datos
        # Esto deberia ser nuestor unico metodo para comunicarnos con la base de datos
        if '_id' not in self.__dict__.keys():
            self.db.insert_one(dict((k, self.__dict__[k]) for k in self.mod_params if k in self.__dict__))
        else:
            self.db.update_one({'_id': self.__dict__['_id']}, {'$set': dict((k, self.__dict__[k]) for k in self.mod_params if k in self.__dict__)}, upsert=False)
            #self.db.update_one({'_id': self.__dict__['_id']},{"$currentDate":{"lastModified":"true"}}, {'$set': dict((k, self.__dict__[k]) for k in self.mod_params if k in self.__dict__)}, upsert=False)

    def update(self, **kwargs):
        # Actualiza los datos. Modificmercancias[0].updatea el objeto (no usamos getters y setters porque no sabemos las posibles variables futuras).
        # Queremos guardar que esa variable ha sido modificada. De modo que en el save solo se guardan los datos modificados.

        for i in kwargs.keys():
            if i not in self.admissible_vars:
                raise ValueError("Admissible Values not admitted. Object not updated.")

        self.__dict__.update(kwargs)
        if ['_id' in self.__dict__.keys()]:
            for key in kwargs.keys():
                self.mod_params.append(key)


    @classmethod
    def query(cls, query):
        """ Devuelve un cursor de modelos
        """
        # TODO
        # cls() es el constructor de esta clase

        cursor = cls.db.aggregate(query)
        cursor = ModelCursor(cls, cursor)

        return cursor


    @classmethod
    def init_class(cls, db, vars_path="model_name.vars"):
        """ Inicializa las variables de clase en la inicializacion del sistema.
        Argumentos:
            db (MongoClient) -- Conexion a la base de datos.
            vars_path (str) -- ruta al archivo con la definicion de variables
            del modelo.
        """
        # TODO
        # cls() es el constructor de esta clase

        with open(vars_path, 'r') as vars_file:
            cls.required_vars = vars_file.readline().split()
            cls.admissible_vars = vars_file.readline().split()
        cls.db = db
        cls.db.create_index([("geolocOrigen", GEOSPHERE), ("geolocDestino", GEOSPHERE)])




#cuardar las variables de peso restante y volumen restante con _ y ya está
class Cliente(object):
    """ Prototipo de la clase modelo
        Copiar y pegar tantas veces como modelos se deseen crear (cambiando
        el nombre Model, por la entidad correspondiente), o bien crear tantas
        clases como modelos se deseen que hereden de esta clase. Este segundo
        metodo puede resultar mas compleja
    """
    required_vars = []
    admissible_vars = []
    db = None

    def __init__(self, **kwargs):
        # **kwargs es un parámetro generico para diccionarios.
        # Mirar si el campo id está, si no está es que es instanciado y no se guarda en las actualizaciones

        for i in self.required_vars:
            if i not in kwargs.keys():
                raise ValueError("Required Values not admitted. Object not updated.")

        self.__dict__.update(kwargs)  # Le pasas un diccionario y lo actualiza (lo rellena)
        #self.__dict__['mercancias'] = []

        if '_id' not in kwargs.keys():
            self.mod_params = list(self.__dict__.keys())
        else:
            self.mod_params = []

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.nombre == other.nombre

    def save(self):
        # imprimirlo por pantalla como json
        # Actualización de los datos en la base de datos
        # Esto deberia ser nuestor unico metodo para comunicarnos con la base de datos
        if '_id' not in self.__dict__.keys():
            self.db.insert_one(dict((k, self.__dict__[k]) for k in self.mod_params if k in self.__dict__))
        else:
            #self.db.update_one({'_id': self.__dict__['_id']}, {"$currentDate":{"lastModified":"true"}}, {'$set': dict((k, self.__dict__[k]) for k in self.mod_params if k in self.__dict__)}, upsert=False)
            self.db.update_one({'_id': self.__dict__['_id']}, {'$set': dict((k, self.__dict__[k]) for k in self.mod_params if k in self.__dict__)}, upsert=False)


    def update(self, **kwargs):
        # Actualiza los datos. Modifica el objeto (no usamos getters y setters porque no sabemos las posibles variables futuras).
        # Queremos guardar que esa variable ha sido modificada. De modo que en el save solo se guardan los datos modificados.

        for i in kwargs.keys():
            if i not in self.admissible_vars:
                raise ValueError("Admissible Values not admitted. Object not updated.")

        self.__dict__.update(kwargs)
        if ['_id' in self.__dict__.keys()]:
            for key in kwargs.keys():
                self.mod_params.append(key)

    @classmethod
    def query(cls, query):
        """ Devuelve un cursor de modelos
        """
        # TODO
        # cls() es el constructor de esta clase

        cursor = cls.db.aggregate(query)
        cursor = ModelCursor(cls, cursor)

        return cursor

    @classmethod
    def init_class(cls, db, vars_path="model_name.vars"):
        """ Inicializa las variables de clase en la inicializacion del sistema.
        Argumentos:
            db (MongoClient) -- Conexion a la base de datos.
            vars_path (str) -- ruta al archivo con la definicion de variables
            del modelo.
        """
        # TODO
        # cls() es el constructor de esta clase
        with open(vars_path, 'r') as vars_file:
            cls.required_vars = vars_file.readline().split()
            cls.admissible_vars = vars_file.readline().split()
        cls.db = db

class Vagon(object):
    """ Prototipo de la clase modelo
        Copiar y pegar tantas veces como modelos se deseen crear (cambiando
        el nombre Model, por la entidad correspondiente), o bien crear tantas
        clases como modelos se deseen que hereden de esta clase. Este segundo
        metodo puede resultar mas compleja
    """
    required_vars = []
    admissible_vars = []
    db = None

    def __init__(self, **kwargs):
        # **kwargs es un parámetro generico para diccionarios.
        # Mirar si el campo id está, si no está es que es instanciado y no se guarda en las actualizaciones

        for i in self.required_vars:
            if i not in kwargs.keys():
                raise ValueError("Required Values not admitted. Object not updated.")
        self.__dict__['mercancias'] = []
        self.__dict__.update(kwargs)  # Le pasas un diccionario y lo actualiza (lo rellena)
        self.__dict__['_remainingVolume'] = self.volumen
        self.__dict__['_remainingWeight'] = self.peso


        if '_id' not in kwargs.keys():
            self.mod_params = list(self.__dict__.keys())
        else:
            self.mod_params = []

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.id == other.id

    def save(self):
        # imprimirlo por pantalla como json
        # Actualización de los datos en la base de datos
        # Esto deberia ser nuestor unico metodo para comunicarnos con la base de datos

        if '_id' not in self.__dict__.keys():
            json = dict()
            for key in self.__dict__.keys():
                if key in self.mod_params:
                    if type(self.__dict__[key]) is list:
                        if key not in json.keys():
                            json[key] = []
                        mercancias = self.__dict__[key]
                        for position in mercancias:
                            if 'mod_params' in position.__dict__:
                                mercancia = {k: position.__dict__[k] for k in position.__dict__['mod_params'] if k in position.__dict__}
                                json[key].append(mercancia)
                            else:
                                json[key].append(position)
                    else:
                        json[key] = self.__dict__[key]
            self.db.insert_one(json)
        #update
        else:
            data = dict((k, self.__dict__[k]) for k in self.mod_params if k in self.__dict__)
            #self.db.update_one({'_id': self.__dict__['_id']}, {"$currentDate":{"lastModified":"true"}}, {'$set': data}, upsert=False)
            self.db.update_one({'_id': self.__dict__['_id']}, {'$set': data}, upsert=False)


    def update(self, **kwargs):
        # Actualiza los datos. Modifica el objeto (no usamos getters y setters porque no sabemos las posibles variables futuras).
        # QUeremos guardar que esa variable ha sido modificada. De modo que en el save solo se guardan los datos modificados.

        for i in kwargs.keys():
            if i not in self.admissible_vars:
                raise ValueError("Admissible Values not admitted. Object not updated.")

        self.__dict__.update(kwargs)
        if ['_id' in self.__dict__.keys()]:
            for key in kwargs.keys():
                if key not in self.mod_params:
                    self.mod_params.append(key)


    def assignMercancia(self, mercancia):
        # if mercancia in self.mercancias:
        #     raise ValueError("Mercancia already in Vagon.")

        if self._remainingVolume > mercancia.volumen:
            if self._remainingWeight > mercancia.peso:
                self._remainingVolume -= mercancia.volumen
                self._remainingWeight -= mercancia.peso
                self.mercancias.append(mercancia)
                self.update(mercancias=self.__dict__['mercancias'])
            else:
                print("Weight not admitted. Mercancia not added.")
                #raise ValueError("Weight not admitted. Mercancia not added.")
        else:
            print("Volume not admitted. Mercancia not added.")
            #raise ValueError("Volume not admitted. Mercancia not added.")



    def unassignMercancia(self, mercancia):
        if mercancia in self.mercancias:
            self._remainingWeight += mercancia.peso
            self._remainingVolume += mercancia.volumen
            self.mercancias.remove(mercancia)
            self.update(mercancias = self.__dict__['mercancias'])
            self.save()
        else:
            raise ValueError("Mercancia not found.")

    @classmethod
    def query(cls, query):
        """ Devuelve un cursor de modelos
        """
        # TODO
        # cls() es el constructor de esta clase

        cursor = cls.db.aggregate(query)
        cursor = ModelCursor(cls, cursor)

        return cursor

    @classmethod
    def init_class(cls, db, vars_path="model_name.vars"):
        """ Inicializa las variables de clase en la inicializacion del sistema.
        Argumentos:
            db (MongoClient) -- Conexion a la base de datos.
            vars_path (str) -- ruta al archivo con la definicion de variables
            del modelo.
        """
        # TODO
        # cls() es el constructor de esta clase
        with open(vars_path, 'r') as vars_file:
            cls.required_vars = vars_file.readline().split()
            cls.admissible_vars = vars_file.readline().split()
        cls.db = db


#CONSULTAS:
# Q1: Listado de todas las mercancías de un cliente.
nombre_cliente = 'Cafes el amanencer'
Q1 = [{'$match': {'nombre': nombre_cliente}}]
#db.Mercancia.aggregate([{"$match":{"nombre":"Cafes el amanencer"}}])


# Q2: Listado de mercancias con origen y destino que se indiquen.
origen = "Cadiz"
destino = "Castellon"
Q2 = [{"$match": {"$and": [ {"origen": origen}, {"destino": destino}]}}]
#db.Mercancia.aggregate([{"$match" : {"$and" : [ {"origen":"Cadiz"}, {"destino" : "Castellon"}]}}])


# Q3: Calcular el peso y volumen total de las mercancías de un cliente un día determinado
#dia = timedate.timedate(2003, 11, 24)
dia = "ISODate(2003-11-24)"
cliente = "Cafes el amanencer"
Q3 = [{"$match":{"nombre": cliente, "fecha": dia}}, {"$group": {"_id": {"nombre": "$nombre", "fecha":"$fecha"}, "totalPeso":{ "$sum":"$peso"}, "totalVolumen" :{"$sum": "$volumen"}}}]
#db.Mercancia.aggregate([{"$match":{"nombre":"Cafes el amanencer", "fecha":ISODate("2003-11-24")}}, {"$group":{_id:{"nombre":"$nombre", "fecha":"$fecha"}, totalPeso:{$sum:"$peso"}, totalVolumen:{$sum:"$volumen"}}}])


# Q4: Calcular el densidad media de las mercancías de un cliente a lo largo de un año.
cliente = "Cafes el amanencer"
anho = 2001
Q4 = [{"$match": {"nombre": cliente}}, {"$project": {"nombre":"$nombre", "peso":"$peso", "volumen":"$volumen", "year":{"$year":"$fecha"}}}, {"$match":{"year": anho}}, {"$group": {"_id": {"nombre": "$nombre", "year":"$year"}, "densidad": {"$avg":{"$divide":["$peso", "$volumen"]}}}}]
#db.Mercancia.aggregate([{"$match":{"nombre":"Cafes el amanencer"}}, {$project: {nombre:"$nombre", peso:"$peso", volumen:"$volumen", year:{$year:"$fecha"}}}, {"$match":{"year":2001}}, {$group: {_id:{nombre:"$nombre", year:"$year"}, densidad:{$avg:{$divide:["$peso", "$volumen"]}}}}])


# Q5: Calcular el número medio de envíos por mes entre un origen y un destino.
origen = "Cadiz"
destino = "Castellon"
Q5 = [{"$match": {"origen": origen, "destino": destino}}, {"$project": {"mes": {"$month": "$fecha"}, "anho": {"$year": "$fecha"}}}, {"$group": {"_id": {"mes": "$mes", "anho": "$anho"}, "envios": {"$sum": 1}}}, {"$group": {"_id": "$_id.mes", "totalEnvios": {"$sum": "$envios"}, "anhosTotales": {"$sum": 1}}}, {"$project": {"_id": 0, "mes": "$_id", "mediaEnvios": {"$divide": ["$totalEnvios", "$anhosTotales"]}}}]
#db.Mercancia.aggregate([{"$match": {"origen": "Cadiz", "destino": "Castellon"}}, {"$project": {"mes": {"$month": "$fecha"}, "anho": {"$year": "$fecha"}}}, {"$group": {"_id": {"mes": "$mes", "anho": "$anho"}, "envios": {"$sum": 1}}}, {"$group": {"_id": "$_id.mes", "totalEnvios": {"$sum": "$envios"}, "anhosTotales": {"$sum": 1}}}, {"$project": {"_id": 0, "mes": "$_id", "mediaEnvios": {"$divide": ["$totalEnvios", "$anhosTotales"]}}}])


# Q6: Listado con los tres destinos con más envíos y número de envíos para un origen determinado y un año determinado
origen = "Cadiz"
anho = 2003
cantidad_destinos = 3
Q6 = [{"$match": {"origen": origen}}, {"$project":{"origen":"$origen", "destino":"$destino", "anho":{"$year": "$fecha"}}}, {"$match": {"anho": anho}}, {"$group":{"_id":"$destino", "envios":{"$sum":1}}}, {"$sort":{"envios":-1}}, {"$limit": cantidad_destinos}]
#db.Mercancia.aggregate([{$match:{"origen": "Cadiz"}}, {"$project":{"origen":"$origen", "destino":"$destino", anho:{"$year": "$fecha"}}}, {$match:{"anho":2003}}, {$group:{_id:"$destino", envios:{$sum:1}}}, {$sort:{"envios":-1}}, {$limit:3}])


# Q7: Listado de mercancías con destino cerca de unas coordenadas determinadas (100km de distancia máxima) ordenadas por orden de distancia.
latitud = -16.2536378
longitud = 28.4597823
distancia = 100000
Q7 = [{"$geoNear": {"near": { "type": "Point", "coordinates": [ latitud,  longitud]}, "maxDistance": distancia, "spherical": "true", "distanceField": "distancia", "distanceMultiplier": 0.001, "includeLocs": "geolocDestino"}}, {"$sort": {"distancia": 1}}]
#db.Mercancia.aggregate([{$geoNear: {near: { type: "Point", coordinates: [ -16.2536378, 28.4597823 ]}, maxDistance: 100000, spherical: true, distanceField: "distancia", distanceMultiplier: 0.001, includeLocs: "geolocDestino"}}, {$sort: {"distancia": 1}}])
#Por algún motivo repite coordenadas para mismos lugares.

# Q8: Listado de mercancías existentes en un vagón (mostrar todos los atributos de las mercancías)
id_vagon = 1330
Q8 = [{"$match":{"id":id_vagon}}, {"$project":{"_id": 0, "mercancias": 1}}]
#db.Vagon.aggregate([{"$match":{id:1330}}, {$project:{_id:0, mercancias:1}}]).pretty()


# Q9: Tipos de mercancías existentes en un vagón y número de mercancías de cada tipo.
id_vagon = 1330
Q9 = [{"$match": {"id": id_vagon}}, {"$unwind": "$mercancias"}, {"$group": {"_id": "$mercancias.tipo", "count": {"$sum": 1}}}]
#db.Vagon.aggregate([{"$match":{id:1330}}, {"$unwind":"$mercancias"}, {$group:{_id:"$mercancias.tipo", count:{$sum:1}}}])



if __name__ == '__main__':
    import json

    client = MongoClient()
    db = client['Practica1']

    Mercancia.init_class(db['Mercancia'], "MercanciaVariables.txt")
    Cliente.init_class(db['Cliente'], "ClienteVariables.txt")
    Vagon.init_class(db['Vagon'], "VagonVariables.txt")

    cursor = db.Mercancia.aggregate(Q3)

    while cursor.alive:
        document = cursor.next()
        print(document)



    #RELLENAR
    # dataFile = open('tesm.data', 'r')
    # data = json.load(dataFile)
    #
    # Mercancia.init_class(db['Mercancia'], "MercanciaVariables.txt")
    # Cliente.init_class(db['Cliente'], "ClienteVariables.txt")
    # Vagon.init_class(db['Vagon'], "VagonVariables.txt")
    #
    # vagones = []
    # clientes   = []
    #
    #
    # for line in data:
    #     cl = Cliente(**line['cliente'])
    #     if cl not in clientes:
    #         clientes.append(cl)
    #         cl.save()
    #
    #     merc = Mercancia(**line['mercancia'])
    #     merc.update(**{"nombre": cl.nombre})
    #     merc.save()
    #
    #     vag = Vagon(**line['vagon'])
    #     if vag not in vagones:
    #         vag.assignMercancia(merc)
    #         vagones.append(vag)
    #     else:
    #         for vagon in vagones:
    #             if vagon == vag:
    #                 vagon.assignMercancia(merc)
    #
    # for vagon in vagones:
    #     vagon.save()
    #RELLENAR
