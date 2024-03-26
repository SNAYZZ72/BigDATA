# Charger la bibliothèque RJDBC
library(RJDBC)

# Charger le driver JDBC pour Hive
drv <- JDBC("org.apache.hive.jdbc.HiveDriver", "/usr/local/apache-hive-3.1.3-bin/jdbc/hive-jdbc-3.1.3-standalone.jar")

# Spécifier l'URL de connexion à Hive
url <- "jdbc:hive2://localhost:10000/"

# Spécifier le nom d'utilisateur et le mot de passe
user <- "oracle"
password <- "welcome1"

# Établir la connexion à Hive
conn <- dbConnect(drv, url, user=user, password=password)

# Définir le chemin vers les données
chemin_data <- "/vagrant/projet/data/"

clean_text_columns <- function(df) {
  # Convertir toutes les colonnes texte en utilisant l'encodage UTF-8
  text_cols <- sapply(df, is.character)
  df[text_cols] <- lapply(df[text_cols], function(x) iconv(x, "latin1", "ASCII//TRANSLIT"))

  df
}

# Fonction pour charger des données
load_data <- function(file_name) {
  data_path <- file.path(chemin_data, file_name)
  if(!file.exists(data_path)) {
    message("Le fichier ", data_path, " n'existe pas.")
    return(NULL)
  }
  data <- read.csv(data_path, encoding = "UTF-8")
  data <- clean_text_columns(data)
  return(data)
}

# Charger les données de CO2
co2_output <- load_data("CO2_output.csv")
co2_output$marque <- tolower(co2_output$marque)

# Charger les données du catalogue
catalogue <- load_data("Catalogue.csv")
catalogue$marque <- tolower(catalogue$marque)

# Vérification rapide pour s'assurer que la colonne 'marque' existe dans les deux jeux de données
if (!"marque" %in% names(co2_output) | !"marque" %in% names(catalogue)) {
  stop("La colonne 'marque' n'existe pas dans l'un des jeux de données.")
}

# Fusionner les données sur la colonne 'marque'
merged_data <- merge(catalogue, co2_output, by = "marque")

# Afficher les 6 premières lignes pour vérifier
head(merged_data)

# Définir le nom de la table Hive où vous souhaitez sauvegarder les données
table_name <- "CATALOGUE_CO2_MERGE"

# Exporter les données fusionnées vers un fichier CSV temporaire
temp_file <- tempfile(fileext = ".csv")
write.csv(merged_data, temp_file, row.names = FALSE, quote = TRUE)

dbSendUpdate(conn, paste0("CREATE TABLE IF NOT EXISTS ", table_name, "
(marque STRING, 
nom STRING, 
puissance STRING, 
longueur STRING, 
nbPlaces STRING, 
nbPortes STRING, 
couleur STRING, 
occassion STRING, 
prix STRING, 
bonusMalus STRING, 
rejet STRING, 
coutEnergie STRING)", 
"ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
TBLPROPERTIES ('skip.header.line.count'='1')"))

# Construire la commande HiveQL pour charger le fichier CSV dans Hive
# Assurez-vous de spécifier le bon chemin d'accès au fichier CSV dans votre système de fichiers Hive
load_command <- sprintf("LOAD DATA LOCAL INPATH '%s' INTO TABLE %s", temp_file, table_name)

# Exécuter la commande HiveQL via JDBC
dbSendUpdate(conn, load_command)

# Supprimer le fichier CSV temporaire si désiré
file.remove(temp_file)

# Fermer la connexion à Hive
dbDisconnect(conn)