# Installer le package RJDBC
# options(repos = "https://cran.rstudio.com/")
# install.packages("RJDBC")

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

# Exécuter la requête SQL pour récupérer la table externe MARKETING_EXT
query <- "SELECT * FROM MARKETING_EXT"
marketing_data <- dbGetQuery(conn, query)

# Afficher la table MARKETING_EXT dans la console
print(marketing_data)

# Fermer la connexion à Hive
dbDisconnect(conn)


