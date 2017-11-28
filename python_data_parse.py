import glob, os
import csv
import tarfile
from os.path import basename

import time
import datetime

import sqlite3

def create_table_accel(db):
    if not db:
        return False

	query = '''CREATE TABLE IF NOT EXISTS accel(diffSecs real,N_samples real,x_mean real,x_absolute_deviation real,x_standard_deviation real,x_max_deviation real,x_PSD_1 real,x_PSD_3 real,x_PSD_6 real,x_PSD_10 real,y_mean real,y_absolute_deviation real,y_standard_deviation real,y_max_deviation real,y_PSD_1 real,y_PSD_3 real,y_PSD_6 real,y_PSD_10 real,z_mean real,z_absolute_deviation real,z_standard_deviation real,z_max_deviation real,z_PSD_1 real,z_PSD_3 real,z_PSD_6 real,z_PSD_10 real,time real)'''

	db.execute(query)

def create_table_audio(db):
	if not db:
		return False

	query = '''CREATE TABLE IF NOT EXISTS audio(diffSecs real,absolute_deviation real,standard_deviation real,max_deviation real,PSD_250 real,PSD_500 real,PSD_1000 real,PSD_2000 real,MFCC_1 real,MFCC_2 real,MFCC_3 real,MFCC_4 real,MFCC_5 real,MFCC_6 real,MFCC_7 real,MFCC_8 real,MFCC_9 real,MFCC_10 real,MFCC_11 real,MFCC_12 real,time real)'''
	db.execute(query)

def create_table_gps(db):
	if not db:
		return False

	query = '''CREATE TABLE IF NOT EXISTS gps(diffSecs real,latitude real,longitude real,altitude real,time real)'''
	db.execute(query)

def create_table_compass(db):
	if not db:
		return False

	query = '''CREATE TABLE IF NOT EXISTS compass(diffSecs real,level real,azimuth_mean real,azimuth_absolute_deviation real,azimuth_standard_deviation real,azimuth_max_deviation real,pitch_mean real,pitch_absolute_deviation real,pitch_standard_deviation real,pitch_max_deviation real,roll_mean real,roll_absolute_deviation real,roll_standard_deviation real,roll_max_deviation real,time real)'''
	db.execute(query)

def insert(db, record, record_type):
	if not db:
		return False

	file_types = {'_accel_':'accel', '_cmpss_':'compass', '_gps_':'gps', '_audio_':'audio'}

	table_name = file_types[record_type]

	rec_len = len(record)
	last_elm = rec_len-1
	ts = record[last_elm]

	date_time = ts
	pattern = '%Y-%m-%d %H:%M:%S'
	epoch = int(time.mktime(time.strptime(date_time, pattern)))

	record[last_elm] = str(epoch)

	values = ','.join(record)
	query = "INSERT INTO " + table_name + " VALUES (" + values +")"
	db.execute(query)
	db.commit()
	return True

def parse_csv_file(file_path, type):
	pass

def check_is_allowed_file(file_name):
	allowed_files = ['_accel_', '_cmpss_', '_gps_', '_audio_']

	for file_type in allowed_files:
		if file_type in file_name:
			return file_type

	return False


db = sqlite3.connect('data.db')

create_table_accel(db)
create_table_audio(db)
create_table_gps(db)
create_table_compass(db)

os.chdir ("D:/test/")
i=0
rootdir = "D:/test/"




#read a content of a directory
for file in glob.glob(rootdir+"*.bz2"):
    foldername = basename(file[:-8])

    os.mkdir(foldername)
    t = tarfile.open(file, 'r')
    t.extractall(foldername)

    csv_files = glob.glob(rootdir+foldername+"/"+foldername+"/*.csv")

    for csvfile in csv_files:
        if "_accel_" in csvfile:
            print("reading csv files ... ")
            for ind_file in csv_files:
				allowed_file_type = check_is_allowed_file(ind_file)

				if not allowed_file_type:
					continue

				with open(ind_file, 'rb') as c_file:
					rdr = csv.reader(c_file, delimiter=',')
					rows = list(rdr)

					count = 0

					for r in rows:
						count += 1
						
						if count == 1:
							continue
						print(r)
						print("inserting into " + allowed_file_type)
						insert(db, r, allowed_file_type)