# spark-task

This repository contains ETL job (ETL_task.py), which should be implemented in Spark

The logic of the ETL Job is:
- to prepare restaurants.csv dataset, 
- assign lattitude and lontitude coordinates to locations, which do not have it, using OpenCageData API
- to prepare weather dataset, in particular, check for duplicates
- enrich restaurants dataset with weather data
- and save final dataset in parquet format

Documentation with screenshots is located in pdf. file
