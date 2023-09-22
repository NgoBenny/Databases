import sqlite3
import Lab1PART1
from Lab1PART1 import Database

class QueryBuilder:
    def __init__(self):
        self.query = ""

    def select(self, table_name, columns="*", where=None):
        self.query = "SELECT {} FROM {}".format(columns, table_name)
        if where:
            self.query += " WHERE {}".format(where)
        return self.query

    def insert(self, table_name, columns, values):
        value_placeholders = ", ".join("?" * len(values))
        self.query = f"INSERT INTO {table_name} ({columns}) VALUES ({value_placeholders})"
        return self.query

    def update(self, table_name, set_values, where=None):
        self.query = "UPDATE {} SET {}".format(table_name, set_values)
        if where:
            self.query += " WHERE {}".format(where)
        return self.query

    def delete(self, table_name, where):
        self.query = "DELETE FROM {} WHERE {}".format(table_name, where)
        return self.query


# create an instance of QueryBuilder
qb = QueryBuilder()

# select all columns from the 'co2' table
query = qb.select('co2')
print(query) # SELECT * FROM co2

# select only the 'year' and 'co2' columns where year = 2021
query = qb.select('co2', 'year, co2', 'year = 2021')
print(query) # SELECT year, co2 FROM co2 WHERE year = 2021

# insert a new row into the 'sea_level' table
query = qb.insert('sea_level', 'year, TOPEX_Poseidon, Jason_1, Jason_2, JASON_3', (2023, 1.0, 2.0, 3.0, 4.0))
print(query) # INSERT INTO sea_level (year, TOPEX_Poseidon, Jason_1, Jason_2, JASON_3) VALUES (?, ?, ?, ?, ?)

# update the 'TOPEX_Poseidon' column in the 'sea_level' table where year = 2020
query = qb.update('sea_level', 'TOPEX_Poseidon = 5.0', 'year = 2020')
print(query) # UPDATE sea_level SET TOPEX_Poseidon = 5.0 WHERE year = 2020

# delete rows from the 'co2' table where co2 > 400
query = qb.delete('co2', 'co2 > 400')
print(query) # DELETE FROM co2 WHERE co2 > 400

db = Database()

# select all rows from the 'co2' table
query = qb.select('co2')
rows = db.curser.execute(query).fetchall()
print(rows)

# insert a new row into the 'sea_level' table
query = qb.insert('sea_level', 'year, TOPEX_Poseidon, Jason_1, Jason_2, JASON_3', (2023, 1.0, 2.0, 3.0, 4.0))
db.curser.execute(query, (2023, 1.0, 2.0, 3.0, 4.0))
db.db.commit()

# update the 'TOPEX_Poseidon' column in the 'sea_level' table where year = 2020
query = qb.update('sea_level', 'TOPEX_Poseidon = 5.0', 'year = 2020')
db.curser.execute(query)
db.db.commit()

# delete rows from the 'co2' table where co2 > 400
query = qb.delete('co2', 'co2 > 400')
db.curser.execute(query)
db.db.commit()
