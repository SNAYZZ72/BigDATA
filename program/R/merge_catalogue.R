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

# Fonction pour enregistrer les données fusionnées
enregistrer_dataframe <- function(dataframe, file_name) {
  data_path <- file.path(chemin_data, file_name)
  write.csv(dataframe, file = data_path, row.names = FALSE, quote = FALSE)
}

# Enregistrer les données fusionnées
enregistrer_dataframe(merged_data, "Catalogue_CO2_merge.csv")