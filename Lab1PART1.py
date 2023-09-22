from bs4 import BeautifulSoup
import re
from collections import defaultdict
import pandas as pd
import sqlite3

with open("Co2.html", "r") as f:
    soup = BeautifulSoup(f, "html.parser")

annual_co2 = defaultdict(int)

for row in soup.find_all("tr"):
    match = re.search(r"<td>(\d{4})</td><td>\d+</td><td>\d{4}\.\d{3}</td><td>(\d+\.\d+)</td>", str(row))
    '''
     r"<td>(\d{4})</td>, Match 4 digits and capture as group 1
    <td>\d+</td>, Match one or more digits
    <td>\d{4}\.\d{3}</td> Match 4 digits, a period, and 3 more digits
    <td>(\d+\.\d+)</td>, Match one or more digits, a period, one or more digits and capture as group 2
    '''
    if match:
        year, month_co2 = match.groups()
        annual_co2[year] += int(float(month_co2))

data = pd.read_csv("SeaLevel.csv", delimiter=",", header=None,
                   names=["year", "TOPEX/Poseidon", "Jason-1", "Jason-2", "Jason-3"],
                   skiprows=4)

data["year"] = data["year"].apply(lambda x: int(str(x).split(".")[0]))

annual_data = data.groupby("year").mean().round(2)

annual_dict = defaultdict(list)

for year, row in annual_data.iterrows():
    for col in annual_data.columns:
        annual_dict[year].append(row[col])

print(annual_co2)
print(annual_dict)

class Database:
    def __init__(self):
        self.db = sqlite3.connect("co2_sea_level.db")
        self.curser = self.db.cursor()

    def create_table(self):
        self.curser.execute("""CREATE TABLE IF NOT EXISTS co2 (
                            year INTEGER PRIMARY KEY,
                            co2 INTEGER)""")
        self.curser.execute("""CREATE TABLE IF NOT EXISTS sea_level
        (
                            year INTEGER PRIMARY KEY,
                            TOPEX_Poseidon REAL,
                            Jason_1 REAL,
                            Jason_2 REAL,
                            JASON_3 REAL)""")
        self.db.commit()

    def insert_co2_data(self, data):
        for year, co2_level in data.items():
            self.curser.execute("INSERT OR IGNORE INTO co2 VALUES (?, ?)",
                                (year, co2_level))
            self.db.commit()

    def insert_sea_level_data(self,data):
        for year, values in data.items():
            self.curser.execute("INSERT OR IGNORE INTO sea_level VALUES"
                                "(?,?,?,?,?)", (year, *values))
            self.db.commit()

    def search_co2_data(self, year):
        self.curser.execute("SELECT co2 FROM co2 WHERE year=?", (year,))
        return self.curser.fetchone()

    def search_sea_level_data(self, year):
        self.curser.execute("SELECT * FROM sea_level WHERE year=?", (year,))
        return self.curser.fetchall()

    def delete_co2_data(self, year):
        self.curser.execute("DELETE FROM co2 WHERE year = {}".format(year))
        self.db.commit()

    def delete_sea_level_data(self, year):
        self.curser.execute("DELETE FROM sea_level WHERE year = {}".format(year))
        self.db.commit()


db = Database()
db.create_table()

db.insert_co2_data(annual_co2)
db.insert_sea_level_data(annual_dict)

print("CO2 Data:")
for year in annual_co2.keys():
    print(f"Year: {year}, CO2 Level: {db.search_co2_data(year)}")

print("Sea Level Data:")
for year in annual_dict.keys():
    print(f"Year: {year}, Sea Level Data: {db.search_sea_level_data(year)}")
