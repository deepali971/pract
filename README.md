from cassandra.cluster import Cluster
# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
keyspace_name = 'College'
replication_options = {
 'class': 'SimpleStrategy',
 'replication_factor': 1
}
create_keyspace_query = f"""
 CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
 WITH REPLICATION = {str(replication_options)}
"""
session.execute(create_keyspace_query)
print("Keyspace Created")
create_query=session.prepare("CREATE TABLE College.Student (id int PRIMARY KEY, name text, 
address text)")
session.execute(create_query)
print("Table Created")
session.execute("INSERT INTO College.Student (id, name, address) VALUES (1, 'Pradnya', 
'Maharashtra')")
session.execute("INSERT INTO College.Student (id, name, address) VALUES (2, 'Rushi', 'Mumbai')")
session.execute("INSERT INTO College.Student (id, name, address) VALUES (3, 'Kinjal', 'Nashik')")
print("Data Inserted")
select_query = "SELECT * FROM College.Student"
result = session.execute(select_query)
print("Student Details before update")
for row in result:
 print(f" ID: {row.id}, Name: {row.name}, Address: {row.address}")
update_query = session.prepare("UPDATE College.Student SET address = 'lonavala' WHERE id = 1")
session.execute(update_query)
print("Data Updated")
print("Data after update")
select_query1 = "SELECT * FROM College.Student"
result = session.execute(select_query1)
print("Student Details")
for row in result:
 print(f" ID: {row.id}, Name: {row.name}, Address: {row.address}")
NAME: PRADNYA RAJENDRA WADKAR 
ROLL NO.:033
SUBJECT: APPLIED BIG DATA ANALYTICS THEORY
session.shutdown()
cluster.shutdown()
