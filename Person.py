from neo4j_db import Database


#stvara cvor 'class_name' sa atributima 'parameters'
#class_name je string, a attributes je dictionary
def create_node(class_name, attributes):
    i = 0
    query_string = 'CREATE(label' + ':' + class_name + '{'
    for attribute in attributes:
        if i == 0:
            query_string += attribute + ': "' + attributes[attribute] + '"'
        else:
            query_string += ', ' + attribute + ': "' + attributes[attribute] + '"'
        i += 1
    query_string += '}) RETURN label'

    db = Database("bolt://localhost:7687", "neo4j", "password")
    return db.query(query_string)

#first_node and second_node are dictionaries, relationship_name is a string
def create_relationship(first_node_name, first_node, second_node_name, second_node, relationship_name):
    i = 0
    j = 0
    query_string = 'MATCH(a:' + first_node_name + '),(b:' + second_node_name + ') WHERE '
    for attribute in first_node:
        if i == 0:
            query_string += 'a.' + attribute + ' = "' + first_node[attribute] + '"'
        else:
            query_string += ' AND a.' + attribute + ' = "' + first_node[attribute] + '"'
        i += 1
    for attribute in second_node:
        if j == 0:
            if i > 0:
                query_string += ' AND '
            query_string += 'b.' + attribute + ' = "' + second_node[attribute] + '"'
        else:
            query_string += ' AND b.' + attribute + ' = "' + second_node[attribute] + '"'
        j += 1
    query_string += ' CREATE(a)-[r:' + relationship_name + ']->(b) RETURN r'

    db = Database("bolt://localhost:7687", "neo4j", "password")
    return db.query(query_string)

#klasa je ime čvora, parametri je dictionary sa where parametrima
def get_nodes(class_name, attributes):
    i = 0
    query_string = 'MATCH(label:' + class_name + '{'
    for attribute in attributes:
        if i == 0:
            query_string += attribute + ': "' + attributes[attribute] + '"'
        else:
            query_string += ', ' + attribute + ': "' + attributes[attribute] + '"'
        i += 1
    query_string += '}) RETURN label'

    db = Database("bolt://localhost:7687", "neo4j", "password")
    return db.query(query_string)

#vraća sve čvorove koji se nalaze u bazi
def get_all():
    query_string = 'MATCH (a) RETURN (a)'
    db = Database("bolt://localhost:7687", "neo4j", "password")
    return db.query(query_string)

#returns a list of properties on the node
#known_properties is a dictionary of at least one node property
#e.g. MATCH (a) WHERE a.name = '' RETURN keys(a)
def get_attributes(known_properties):
    i = 0
    query_string = 'MATCH(a) WHERE a.'
    for known_property in known_properties:
        if i == 0:
            query_string += known_property + ' = "' + known_properties[known_property] + '"'
        else:
            query_string += ' AND a.' + known_property + ' = "' + known_properties[known_property] + '"'
    query_string += 'RETURN keys(a)'

    db = Database("bolt://localhost:7687", "neo4j", "password")
    result = db.query(query_string)
    return result[0]['keys(a)']


#creates a node only if the node with the same name and exact same attributes doesn't already exist
# if it exists, that node is returned
#e.g. MERGE (charlie { name: 'Charlie Sheen', age: 10 }) RETURN charlie
def create_if_not_exist(class_name, attributes):
    i = 0
    query_string = 'MERGE(label' + ':' + class_name + '{'
    for attribute in attributes:
        if i == 0:
            query_string += attribute + ': "' + attributes[attribute] + '"'
        else:
            query_string += ', ' + attribute + ': "' + attributes[attribute] + '"'
        i += 1
    query_string += '}) RETURN label'

    db = Database("bolt://localhost:7687", "neo4j", "password")
    return db.query(query_string)


def get_requests_for_ip(ip_address):
    query_string = 'MATCH(a:Ip {address: "' + ip_address + '"})-[:HAS_SENT]->(b:Request) RETURN b'
    db = Database("bolt://localhost:7687", "neo4j", "password")
    return db.query(query_string)




person = {
    'name': 'Pero Peric',
    'born': '1964'
}
location = {
    "name": "zagreb",
    "postal_code": "10000"
}
ip = {
    "address": "123:123:123:123",
    "ip_type": "ipv4"
}
print(create_node('Person', person))
print(create_node('Location', location))
print(create_node('Ip', ip))
print(create_relationship('Person', person, 'Location', location, 'LIVES_IN'))
print(create_relationship('Ip', ip, 'Location', location, 'LOCATED_IN'))
print(create_relationship('Person', person, 'Ip', ip, 'HAS'))