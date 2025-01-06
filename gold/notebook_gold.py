# Databricks notebook source
# Configurer les paramètres pour Gold
storage_name = "dlkefrei91320"
access_key = "écrire la clé Azure ici"

# Gold
container_gold = "ds-gold"
mount_point_gold = "/mnt/ds-gold"

# Configurer les sources
source_gold = f"wasbs://{container_gold}@{storage_name}.blob.core.windows.net"

configs = {"fs.azure.account.key." + storage_name + ".blob.core.windows.net": access_key}

# Vérifier si Gold est monté
if mount_point_gold not in [mnt.mountPoint for mnt in dbutils.fs.mounts()]:
    dbutils.fs.mount(
        source=source_gold,
        mount_point=mount_point_gold,
        extra_configs=configs
    )
    print(f"Gold monté sur : {mount_point_gold}")
else:
    print(f"Gold est déjà monté sur : {mount_point_gold}")

# Lister les fichiers disponibles dans gold
print("Fichiers disponibles dans ds-gold :")
display(dbutils.fs.ls(mount_point_gold))



# COMMAND ----------

# Databricks notebook for converting CSV files to Delta tables in a star schema architecture.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_delta_table(df, table_name, path):
    """
    Create or overwrite a Delta table with the given DataFrame.
    """
    # Write the DataFrame to Delta format
    df.write.format("delta") \
        .mode("overwrite") \
        .save(path)

    # Register the Delta table in the Metastore
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{path}'")

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Databricks Delta Star Schema") \
    .getOrCreate()

storage_name = "dlkefrei91320"
access_key = "votre clé d'accès Azure"
spark.conf.set(f"fs.azure.account.key.{storage_name}.blob.core.windows.net", access_key)


# Define input CSV paths
input_paths = {
    "players": "/mnt/ds-gold/players_cleaned/players_cleaned.csv",
    "player_valuations": "/mnt/ds-gold/player_valuations_cleaned/player_valuations_cleaned.csv",
    "games": "/mnt/ds-gold/games_cleaned/games_cleaned.csv",
}

# Define output paths for Delta tables
output_base_path = "/mnt/gold/delta_tables"
table_paths = {
    "main": f"{output_base_path}/main",
    "player_valuations": f"{output_base_path}/player_valuations",
    "games": f"{output_base_path}/games",
}

# Load input CSV files
players_df = spark.read.csv(input_paths["players"], header=True, inferSchema=True)
player_valuations_df = spark.read.csv(input_paths["player_valuations"], header=True, inferSchema=True)
games_df = spark.read.csv(input_paths["games"], header=True, inferSchema=True)

# Alias DataFrames to avoid ambiguity
players_df = players_df.alias("players")
player_valuations_df = player_valuations_df.alias("player_valuations")
games_df = games_df.alias("games")

# Join data to create the main table
main_table = players_df.select(col("player_id"), col("name"), col("current_club_id").alias("players_current_club_id")) \
    .join(player_valuations_df.select(col("player_id"), col("current_club_id").alias("valuations_current_club_id")), on="player_id", how="left") \
    .join(games_df.select("game_id", "home_club_id", "away_club_id"), col("players_current_club_id") == col("home_club_id"), how="left") \
    .select("player_id", "name", "players_current_club_id", "game_id", "home_club_id", "away_club_id")

# Write main table to Delta
create_delta_table(
    main_table, 
    "main_table", 
    table_paths["main"]
)

# Display first rows of main_table
print("First rows of main_table:")
main_table.show(5)

# Write secondary tables to Delta
create_delta_table(
    player_valuations_df, 
    "player_valuations_table", 
    table_paths["player_valuations"]
)
create_delta_table(
    games_df, 
    "games_table", 
    table_paths["games"]
)

print("Delta tables created in a star schema format.")

# Export Delta tables to Azure Storage
storage_name = "dlkefrei91320"
export_paths = {
    "main": f"wasbs://delta-tables@{storage_name}.blob.core.windows.net/main",
    "player_valuations": f"wasbs://delta-tables@{storage_name}.blob.core.windows.net/player_valuations",
    "games": f"wasbs://delta-tables@{storage_name}.blob.core.windows.net/games",
}

# Save Delta tables to Azure Storage
main_table.write.format("delta").mode("overwrite").save(export_paths["main"])
player_valuations_df.write.format("delta").mode("overwrite").save(export_paths["player_valuations"])
games_df.write.format("delta").mode("overwrite").save(export_paths["games"])

print("Delta tables exported to Azure Storage.")



# COMMAND ----------

# Databricks notebook to calculate win rate and compare with player valuations (Hypothesis 1)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, sum
import matplotlib.pyplot as plt

# Initialize SparkSession
spark = SparkSession.builder.appName("Hypothesis: Win Rate vs Player Valuations").getOrCreate()

# Load existing tables
players_df = spark.table("main_table").alias("players")
player_valuations_df = spark.table("player_valuations_table").alias("valuations")
games_df = spark.table("games_table").alias("games")

# Step 1: Calculate win rate for each club
winrate_df = games_df \
    .groupBy(col("games.home_club_id").alias("club_id")) \
    .agg(
        (sum(when(col("games.home_club_goals") > col("games.away_club_goals"), 1).otherwise(0)) / count("games.game_id")).alias("win_rate"),
        count("games.game_id").alias("total_games")
    )

# Step 2: Enrich players data with valuations
players_with_valuations_df = players_df \
    .join(player_valuations_df, "player_id", "inner")

# Step 3: Join win rate with player valuations
club_performance_df = winrate_df \
    .join(players_with_valuations_df, winrate_df["club_id"] == players_with_valuations_df["players_current_club_id"], "inner") \
    .groupBy("club_id") \
    .agg(
        avg("market_value_in_eur").alias("avg_player_value"),
        avg("win_rate").alias("avg_win_rate")
    )

# Display the results
club_performance_df.show()

# Step 4: Visualization
club_performance_pd = club_performance_df.toPandas()
plt.figure(figsize=(10, 6))
plt.scatter(club_performance_pd["avg_win_rate"], club_performance_pd["avg_player_value"], color="blue")
plt.xlabel("Average Win Rate")
plt.ylabel("Average Player Value (EUR)")
plt.title("Player Value vs. Club Win Rate")
plt.tight_layout()
plt.show()

print("Hypothesis 1 analysis complete.")
