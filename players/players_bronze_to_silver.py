# Databricks notebook source
# Configurer les paramètres pour Bronze, Silver et Gold
storage_name = "dlkefrei91320"
access_key = "écrire la clé Azure ici"

# Bronze
container_bronze = "ds-bronze"
mount_point_bronze = "/mnt/ds-bronze"

# Silver
container_silver = "ds-silver"
mount_point_silver = "/mnt/ds-silver"

# Configurer les sources
source_bronze = f"wasbs://{container_bronze}@{storage_name}.blob.core.windows.net"
source_silver = f"wasbs://{container_silver}@{storage_name}.blob.core.windows.net"

configs = {"fs.azure.account.key." + storage_name + ".blob.core.windows.net": access_key}

# Vérifier si Bronze est monté
if mount_point_bronze not in [mnt.mountPoint for mnt in dbutils.fs.mounts()]:
    dbutils.fs.mount(
        source=source_bronze,
        mount_point=mount_point_bronze,
        extra_configs=configs
    )
    print(f"Bronze monté sur : {mount_point_bronze}")
else:
    print(f"Bronze est déjà monté sur : {mount_point_bronze}")

# Vérifier si Silver est monté
if mount_point_silver not in [mnt.mountPoint for mnt in dbutils.fs.mounts()]:
    dbutils.fs.mount(
        source=source_silver,
        mount_point=mount_point_silver,
        extra_configs=configs
    )
    print(f"Silver monté sur : {mount_point_silver}")
else:
    print(f"Silver est déjà monté sur : {mount_point_silver}")

# Lister les fichiers disponibles dans Bronze
print("Fichiers disponibles dans ds-bronze :")
display(dbutils.fs.ls(mount_point_bronze))

# COMMAND ----------

# Charger les données brutes depuis Bronze
players_df = spark.read.format("csv").option("header", "true").load(f"{mount_point_bronze}/players/")

# Afficher un aperçu des données brutes
print("Données brutes de 'players':")
players_df.show(10)

# COMMAND ----------

# Nettoyage des données 'players'

# 1. Supprimer les colonnes inutiles
columns_to_drop = ['image_url', 'url', 'agent_name', 'contract_expiration_date', 'date_of_birth']
players_cleaned = players_df.drop(*columns_to_drop)

# 2. Supprimer les lignes avec des valeurs nulles dans les colonnes critiques
columns_critical = ['player_id', 'name', 'position', 'current_club_name', 'market_value_in_eur']
players_cleaned = players_cleaned.dropna(subset=columns_critical)

# 3. Supprimer les doublons
players_cleaned = players_cleaned.dropDuplicates()

# 4. Trier les données par position et market_value_in_eur
players_cleaned = players_cleaned.orderBy(["position", "market_value_in_eur"], ascending=[True, False])

# COMMAND ----------

# Afficher les données nettoyées
print("Données nettoyées et triées de 'players':")
players_cleaned.show(10)

# COMMAND ----------

# Sauvegarder les données nettoyées en un seul fichier dans Silver
temp_path = f"{mount_point_silver}/players_cleaned_temp/"
final_path = f"{mount_point_silver}/players_cleaned/players_cleaned.csv"

# Réduire à une seule partition et sauvegarder temporairement
players_cleaned.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(temp_path)

# Identifier et renommer le fichier généré
csv_file = [f.path for f in dbutils.fs.ls(temp_path) if f.name.endswith(".csv")][0]
dbutils.fs.mv(csv_file, final_path)

print(f"Fichier unique sauvegardé dans : {final_path}")

# COMMAND ----------

# Lister les fichiers sauvegardés dans Silver
print("Contenu de ds-silver après sauvegarde :")
display(dbutils.fs.ls(f"{mount_point_silver}/players_cleaned/"))
