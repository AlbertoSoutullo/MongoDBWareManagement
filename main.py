__author__ = 'Alberto Soutullo'

from pymongo import MongoClient


#Queries:
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

    Ware.init_class(db['Mercancia'], "MercanciaVariables.txt")
    Client.init_class(db['Cliente'], "ClienteVariables.txt")
    Wagon.init_class(db['Vagon'], "VagonVariables.txt")

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
