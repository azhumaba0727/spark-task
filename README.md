# spark-task

This repository contains ETL job (ETL_task.py), which should be implemented in Spark

The logic of the ETL Job is:
- to preapre restaurants.csv dataset, 
- assign lattitude and lontitude coordinates to locations, which do not have it, using OpenCageData API
- to prepare weather dataset, in particular, check for duplicates
- enrich restaurants dataset with weather data
- and save final dataset in parquet format

Documentation with scrrenshot is located in dcoumentaion folder