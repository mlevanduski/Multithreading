# -*- coding: utf-8 -*-
"""
Created on Thu Oct 29 16:14:23 2020

@author: MLevanduski
"""
import random
import sqlite3
from sqlite3 import Error
from concurrent.futures import ThreadPoolExecutor,as_completed

#Part II SQLite Database

people_db_file = "sqlite.db" #name of db file to use
max_people = 500 #number of records to create

def create_people_database(db_file, count):
    conn = sqlite3.connect(db_file)
    with conn:
        sql_create_people_table = """CREATE TABLE IF NOT EXISTS people (
            id integer PRIMARY KEY,
            first_name text NOT NULL,
            last_name text NOT NULL);"""
        cursor = conn.cursor()
        cursor.execute(sql_create_people_table)
        #Truncating Data
        sql_truncate_people = "DELETE FROM people;"
        cursor.execute(sql_truncate_people)
        #Create list of person tuples
        people = generate_people(count)
        #Create query to add the people records and execute
        sql_insert_person = "INSERT INTO people(id,first_name,last_name) VALUES(?,?,?);"
        
        for person in people:
            print(person)
            cursor.execute(sql_insert_person, person)
            print(cursor.lastrowid)
        cursor.close()

#Part III context manager class for database

class PersonDB():
    
    def __init__(self, db_file = ''):
        self.db_file = db_file
    
    def __enter__(self):
        conn = sqlite3.connect(self.db_file)
        self.conn = conn
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.conn.close()
        
    def load_person(self, id):
        sql = "SELECT * FROM people WHERE id=?"
        cursor = self.conn.cursor()
        cursor.execute(sql, (id,))
        records = cursor.fetchall()
        result = (-1,'','')
        if records is not None and len(records) > 0:
            result = records[0]
        cursor.close()
        return result

def test_PersonDB():
    with PersonDB(people_db_file) as db:
        print(db.load_person(1000)) # Exceeds max_people should print default
        print(db.load_person(122))
        print(db.load_person(300))

#Part IV helper method that interacts with load_person method in PersonDB class.

def load_person(id, db_file):
    with PersonDB(db_file) as db:
        return db.load_person(id)

#Part I Generate people function 

def generate_people(count):
    last_names = []
    first_names = []
    #Last Names
    with open('LastNames.txt','r') as filehandle:
        for name in filehandle.readlines():
            name = name.rstrip('\n')
            last_names.append(name)
            
    #First Names
    with open('FirstNames.txt','r') as filehandle:
        for name in filehandle.readlines():
            name = name.rstrip('\n')
            first_names.append(name)
    
    #List of tuples
    counter = 0
    names = []
    for tup in range(0,count):
        tuple_obj = (counter, first_names[random.randint(0,len(first_names)-1)], last_names[random.randint(0,len(last_names)-1)])
        names.append(tuple_obj)
        counter += 1
    
    #Returning the list of tuples created above
    return names

#Main function for the script
      
if __name__ == "__main__":
    people = generate_people(5)
    print(people)
    print("="*40 + "\n")
    create_people_database(people_db_file, 400)
    print("="*40 + "\n")
    test_PersonDB()

# Part IV multithreading

    big_list = []
    with ThreadPoolExecutor(max_workers = 10) as executor:
        futures = [executor.submit(load_person,x,people_db_file) for x in range(0,(max_people-1))]
        for future in as_completed(futures):
            big_list.append(future.result())
        print(big_list)
    big_list.sort(key = lambda col : (col[1],col[2])) # dont understand why multisort not working
    for record in big_list:
        print(record)
