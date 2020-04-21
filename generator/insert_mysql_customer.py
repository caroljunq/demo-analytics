# https://pynative.com/python-mysql-insert-data-into-database-table/
import sys
import random
import mimesis
import json
import common_functions
import pymysql as mysql

# reading config
with open('../config.json') as data:
    config = json.load(data)

language = config["language"]

person = mimesis.Person(language)
address = mimesis.Address(language)

# general config
language = config["language"]

# customers config
max_age = config["customers"]["max_age"]
min_age = config["customers"]["min_age"]

# genders config
genders = config["genders"][language]
gender_prob = config["genders"]["percentages"]

# Calculating random probabilities
# 245 countries
country_prob = common_functions.random_probabilities(1,245)
ages_probab  = common_functions.random_probabilities(min_age, max_age)

countries = list(range(1,245 + 1))
ages = list(range(min_age,max_age + 1))

n_inserts = int(sys.argv[1])


connection = mysql.connect(host='hostname',
                            database='database_name',
                            user='username',
                            password='pass'
                        )

mysql_insert_query = """INSERT INTO customers_cdc (customer_id, 
                        age, country_id, gender, 
                        city, state) 
                        VALUES (%s, %s, %s, %s,%s, %s) """

records_to_insert = []

for i in range(n_inserts):
    customer_id = random.getrandbits(128)
    country_id = np.random.choice(countries, p=country_prob)
    age = np.random.choice(ages, p=ages_probab)
    gender = np.random.choice(genders, p=gender_prob)
    city = address.city().replace(',','')
    state = address.state().replace(',','')
    records_to_insert.add((customer_id,age,country_id,gender,city,state))

cursor = connection.cursor()
cursor.executemany(mysql_insert_query, records_to_insert)
connection.commit()
print(cursor.rowcount, "Record inserted successfully into customers_cdc table")

cursor.close()
connection.close()
print("MySQL connection is closed")