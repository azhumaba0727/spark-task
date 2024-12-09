from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, udf, concat
from pyspark.sql.types import DoubleType, StringType
import requests
import geohash

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Spark ETL Job") \
    .master("local[*]") \
    .getOrCreate()

# Define paths
restaurant_data_path = "./restaurant_csv"
weather_data_path = "./weather"
output_path = "./final_dataset"

# Read restaurant data
restaurant_df = spark.read.option("header", "true").csv(restaurant_data_path)

# Filter records with missing latitude or longitude
null_coordinates_df = restaurant_df.filter((col("lat").isNull()) | (col("lng").isNull()))

# Add address column
new_null_coordinates_df = null_coordinates_df.withColumn(
    "address",
    concat(col("franchise_name"), lit(", "), col("city"), lit(", "), col("country"))
)

API_KEY = "e1acc744eda0444aaf710471f776c4a5"

#function to get coordinates
def get_coordinates(address):
    if not address:
        return None, None
    
    url = f"https://api.opencagedata.com/geocode/v1/json?q={address}&key={API_KEY}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            location = data['results'][0]['geometry']
            return location['lat'], location['lng']
    return None, None

# UDF to get latitude and longitude
@udf(DoubleType())
def fetch_latitude(address):
    lat, _ = get_coordinates(address)
    return lat

@udf(DoubleType())
def fetch_longitude(address):
    _, lng = get_coordinates(address)
    return lng

#Add new latitude and longitude
updated_coords_df = new_null_coordinates_df.withColumn("latitude", fetch_latitude(col("address"))).withColumn("longitude", fetch_longitude(col("address")))

# Drop "address" column
new_coords_df = updated_coords_df.drop("address")

# Remove rows with missing lat/lng from the original DataFrame
filtered_df = restaurant_df.join(new_coords_df, on="id", how="left_anti")

# Combine updated coordinates with the original data
final_restaurant_df = filtered_df.unionByName(new_coords_df)

# Add geohash column
def generate_geohash(lat, lng):
    if lat is None or lng is None:
        return None
    return geohash.encode(lat, lng, precision=4)

geohash_udf = udf(generate_geohash, StringType())

final_restaurant_df = final_restaurant_df.withColumn("lat", col("lat").cast("float")).withColumn("lng", col("lng").cast("float")).withColumn("geohash", geohash_udf(col("lat"), col("lng")))

# Read weather data
weather_df = spark.read.parquet(weather_data_path)

# Add geohash to weather data
weather_df_with_geohash = weather_df.withColumn("geohash", geohash_udf(col("lat"), col("lng")))

# Deduplicate weather data by geohash and keep the latest record per geohash
window_spec = Window.partitionBy("geohash").orderBy(col("wthr_date").desc())

deduplicated_weather_df = weather_df_with_geohash.withColumn("rank", F.row_number().over(window_spec)).filter(col("rank") == 1).drop("rank")

# Join restaurant and weather datasets on geohash
final_df = final_restaurant_df.join(deduplicated_weather_df, on="geohash", how="left")

#Renaming same name columns
# Get all column names
column_names = final_df.columns

# Create a renaming map for conflicting columns
renamed_columns = []
seen_columns = set()

for col_name in column_names:
    if col_name in seen_columns:
        # If column name is already seen, append a suffix
        new_name = f"{col_name}_wthr"
        renamed_columns.append(new_name)
    else:
        renamed_columns.append(col_name)
        seen_columns.add(col_name)

# Apply renaming to the DataFrame
renamed_df = final_df.toDF(*renamed_columns)

# Save the final DataFrame partitioned by year, month, and day
renamed_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(output_path)

# Stop the Spark session
spark.stop()



