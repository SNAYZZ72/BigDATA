# Charger les bibliothèques nécessaires
library(RJDBC)
library(dplyr)
library(stringr)
library(cluster) # Peut-être pas nécessaire si on ne fait pas de clustering
library(ggplot2) # Peut-être pas nécessaire si on ne fait pas de visualisation

# Charger le driver JDBC pour Hive
drv <- JDBC("org.apache.hive.jdbc.HiveDriver", "/usr/local/apache-hive-3.1.3-bin/jdbc/hive-jdbc-3.1.3-standalone.jar")

# Spécifier l'URL de connexion à Hive
url <- "jdbc:hive2://localhost:10000/"

# Spécifier le nom d'utilisateur et le mot de passe
user <- "oracle"
password <- "welcome1"

# Établir la connexion à Hive
conn <- dbConnect(drv, url, user=user, password=password)

# Fonction pour convertir les colonnes texte avec encodage spécifique
clean_text_columns <- function(df) {
  text_cols <- sapply(df, is.character)
  df[text_cols] <- lapply(df[text_cols], function(x) iconv(x, from = "UTF-8", to = "ASCII//TRANSLIT"))
  df
}

# Fonction pour nettoyer les données directement via une requête Hive
clean_data_hive <- function(conn, query) {
  data <- dbGetQuery(conn, query)
  
  data <- clean_text_columns(data)
  
  if("marketing_ext.situationfamiliale" %in% names(data)) {
    data <- data %>% mutate(
      marketing_ext.situationfamiliale = str_replace(marketing_ext.situationfamiliale, "Célibataire", "Celibataire")
      )
  }

  if("marketing_ext.taux" %in% names(data)) {
    data <- data %>% mutate(marketing_ext.taux = as.integer(marketing_ext.taux)) %>%
        filter(marketing_ext.taux >= 544 & marketing_ext.taux <= 74185)
  }
  
  # Autres nettoyages spécifiques peuvent être appliqués ici
  
  data
}

# Exemple d'utilisation pour nettoyer les données marketing
marketing_data_query <- "SELECT * FROM marketing_ext"
marketing_data <- clean_data_hive(conn, marketing_data_query)

# Fermer la connexion à Hive
dbDisconnect(conn)

# Sauvegarder les données nettoyées si nécessaire
write.csv(marketing_data, "/vagrant/projet/data/cleaned/Marketing_cleaned.csv", row.names = FALSE)

# Message de fin de nettoyage
message("Nettoyage terminé pour les données spécifiques.")