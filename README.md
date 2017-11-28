The used python script checks in
every folder to see whether it contains both_accel or not.
If the folder contains "_accel", then it creates a database and writes the corresponding hdl_[PARTICIPANT
NAME]_[PHONE IDENTIFIER]_[TIME STAMP OF START RECORDINGYYYYMMDD_HHMMSS]_[TIME
STAMP OF END RECORDINGYYYYMMDD_HHMMSS].csv (Comma-Separated Values) into corresponding
database table. The data preprocessing discards all those folders which do not have accelerometer
data as well as folders with duplicate data and only writes accelerometer data to the database. The data
stored in the database now contains data from the entire data set for the accelerometer. The accelerometer
measures the vibration in three spatial dimensions, which will be a good source to determine the
tremor