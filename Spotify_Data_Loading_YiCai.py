# Databricks notebook source
# Step 1: Read Spotify CSV files
artists = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("escape", '"')
    .csv("/Volumes/workspace/capstone_project/spotify_raw_data/artists.csv")
)
tracks = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("escape", '"')
    .csv("/Volumes/workspace/capstone_project/spotify_raw_data/tracks.csv")
)

print("Artists:", artists.count())
print("Tracks:", tracks.count())

# COMMAND ----------

# Step 2: Prepare ARTIST table

from pyspark.sql.functions import col

artist_data = artists.select(
    col("id").alias("artist_id"),
    col("name").alias("artist_name"),
    col("followers").cast("bigint").alias("followers"),
    col("popularity").cast("int").alias("artist_popularity")
)

artist_data.printSchema()
artist_data.show(5, truncate=False)

# COMMAND ----------

# Load data only when the target table is empty
def load_if_empty(df, table_name):
    full_name = f"workspace.capstone_project.{table_name}"

    if spark.table(full_name).count() == 0:
        df.write.mode("append").saveAsTable(full_name)
        print(f"{table_name.upper()} data loaded successfully")
    else:
        print(f"{table_name.upper()} already contains data. Skipping load.")

    print(f"{table_name.upper()} rows:", spark.table(full_name).count())

# COMMAND ----------

# Step 3: Load data into ARTIST table
load_if_empty(artist_data, "artist")

# COMMAND ----------

# Step 4: Prepare TRACK table

from pyspark.sql.functions import col, to_date

track_data = tracks.select(
    col("id").alias("track_id"),
    col("name").alias("track_name"),
    col("popularity").cast("int").alias("track_popularity"),
    col("duration_ms").cast("int"),
    col("explicit").cast("boolean"),
    to_date(col("release_date"), "yyyy-MM-dd").alias("release_date")
)

track_data.printSchema()
track_data.show(5, truncate=False)

# COMMAND ----------

# Step 5: Load data into TRACK table
load_if_empty(track_data, "track")

# COMMAND ----------

# Step 6: Prepare TRACK_KEY table

from pyspark.sql.functions import col

track_key_data = (
    tracks
    .select(col("key").cast("bigint").alias("key_id"))
    .distinct()
    .withColumn("key_name", col("key_id").cast("string"))
    .orderBy("key_id")
)

track_key_data.show(20, truncate=False)
print("TRACK_KEY rows prepared:", track_key_data.count())

# COMMAND ----------

# Step 7: Load data into TRACK_KEY table
load_if_empty(track_key_data, "track_key")

# COMMAND ----------

# Step 8: Prepare TRACK_FEATURE table

from pyspark.sql.functions import col

track_feature_data = tracks.select(
    col("id").alias("track_id"),
    col("danceability").cast("double"),
    col("energy").cast("double"),
    col("key").cast("bigint").alias("key_id"),
    col("loudness").cast("double"),
    col("mode").cast("boolean").alias("modality"),
    col("speechiness").cast("double"),
    col("acousticness").cast("double"),
    col("instrumentalness").cast("double"),
    col("liveness").cast("double"),
    col("valence").cast("double"),
    col("tempo").cast("double"),
    col("time_signature").cast("int")
)

track_feature_data.printSchema()
track_feature_data.show(5, truncate=False)

# COMMAND ----------

# Step 9: Load data into TRACK_FEATURE table
load_if_empty(track_feature_data, "track_feature")

# COMMAND ----------

# Step 10: Prepare GENRE table

from pyspark.sql.functions import col, explode, split, regexp_replace, trim, row_number
from pyspark.sql.window import Window

artist_genres = (
    artists
    .select(
        col("id").alias("artist_id"),
        explode(
            split(
                regexp_replace(col("genres"), r"""[\[\]'"]""", ""),
                ","
            )
        ).alias("genre_name")
    )
    .withColumn("genre_name", trim(col("genre_name")))
    .filter(col("genre_name") != "")
    .distinct()
)

genre_data = (
    artist_genres
    .select("genre_name")
    .distinct()
    .withColumn(
        "genre_id",
        row_number().over(Window.orderBy("genre_name")).cast("bigint")
    )
    .select("genre_id", "genre_name")
)

genre_data.show(10, truncate=False)
print("GENRE rows prepared:", genre_data.count())

# COMMAND ----------

# Step 11: Load data into GENRE table
load_if_empty(genre_data, "genre")

# COMMAND ----------

# Step 12: Prepare ARTIST_GENRE table

artist_genre_data = (
    artist_genres
    .join(
        spark.table("workspace.capstone_project.genre"),
        on="genre_name",
        how="inner"
    )
    .select("artist_id", "genre_id")
    .distinct()
)

artist_genre_data.show(10, truncate=False)
print("ARTIST_GENRE rows prepared:", artist_genre_data.count())

# COMMAND ----------

# Step 13: Load data into ARTIST_GENRE table
load_if_empty(artist_genre_data, "artist_genre")

# COMMAND ----------

# Step 14: Prepare TRACK_ARTIST table

from pyspark.sql.functions import col, explode, split, regexp_replace, trim

track_artist_data = (
    tracks
    .select(
        col("id").alias("track_id"),
        explode(
            split(
                regexp_replace(col("id_artists"), r"""[\[\]'"]""", ""),
                ","
            )
        ).alias("artist_id")
    )
    .withColumn("artist_id", trim(col("artist_id")))
    .filter(col("artist_id") != "")
    .distinct()
)

track_artist_data.show(10, truncate=False)
print("TRACK_ARTIST rows prepared:", track_artist_data.count())

# COMMAND ----------

# Step 15: Check missing artist references

missing_artist_refs = (
    track_artist_data
    .join(
        spark.table("workspace.capstone_project.artist")
        .select("artist_id"),
        on="artist_id",
        how="left_anti"
    )
)

print("Missing artist references:", missing_artist_refs.count())

# COMMAND ----------

# Step 16: Prepare valid TRACK_ARTIST records

valid_track_artist_data = (
    track_artist_data
    .join(
        spark.table("workspace.capstone_project.artist")
        .select("artist_id"),
        on="artist_id",
        how="left_semi"
    )
    .select("track_id", "artist_id")
)

print("Total relationships:", track_artist_data.count())
print("Unmatched relationships:", missing_artist_refs.count())
print("Valid relationships:", valid_track_artist_data.count())

# COMMAND ----------

## Step 17: Load data into TRACK_ARTIST table
load_if_empty(valid_track_artist_data, "track_artist")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Item 7: Row Count Verification
# MAGIC
# MAGIC SELECT 'ARTIST' AS table_name, COUNT(*) AS row_count
# MAGIC FROM workspace.capstone_project.artist
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'TRACK', COUNT(*)
# MAGIC FROM workspace.capstone_project.track
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'TRACK_FEATURE', COUNT(*)
# MAGIC FROM workspace.capstone_project.track_feature
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ARTIST_GENRE', COUNT(*)
# MAGIC FROM workspace.capstone_project.artist_genre
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'TRACK_ARTIST', COUNT(*)
# MAGIC FROM workspace.capstone_project.track_artist
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'GENRE', COUNT(*)
# MAGIC FROM workspace.capstone_project.genre
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'TRACK_KEY', COUNT(*)
# MAGIC FROM workspace.capstone_project.track_key

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ARTIST Row Count
# MAGIC SELECT COUNT(*) AS artist_count
# MAGIC FROM workspace.capstone_project.artist

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRACK Row Count
# MAGIC SELECT COUNT(*) AS track_count
# MAGIC FROM workspace.capstone_project.track

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRACK_FEATURE Row Count
# MAGIC SELECT COUNT(*) AS track_feature_count
# MAGIC FROM workspace.capstone_project.track_feature