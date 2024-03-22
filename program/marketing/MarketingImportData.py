import csv
import kvstore

class MarketingImportData:
    def __init__(self):
        self.store = kvstore.Client('http://localhost:5000')
        self.tabMarketing = "MARKETINGS"
        self.pathToCSVFile = "../../vagrant/projet/data/Marketing.csv"
        self.clientID = 1
        # récupérer la valeur de l'instance kvstore client, vérifier si elle est déjà initialisée
        try:
            self.store.get(self.tabMarketing)
        except Exception as e:
            print(f"Erreur lors de la récupération de l'instance kvstore client : {e}")
            self.store = kvstore.Client('http://localhost:5000')
            print("Nouvelle instance kvstore client initialisée")

    def main(self):
        try:
            self.init_marketing_tables_and_data()
        except Exception as e:
            print(e)

    def init_marketing_tables_and_data(self):
        self.drop_table_marketing()
        self.create_table_marketing()
        self.load_marketing_data_from_file()

    def display_result(self, result, statement):
        print("===========================")
        print("Résultat de l'opération :")
        print(result)

    def drop_table_marketing(self):
        statement = f"drop table IF EXISTS {self.tabMarketing}"
        self.execute_ddl(statement)

    def create_table_marketing(self):
        statement = (
            f"create table IF NOT EXISTS {self.tabMarketing} ("
            + "ID INTEGER,"
            + "AGE STRING,"
            + "SEXE STRING,"
            + "TAUX STRING,"
            + "SITUATIONFAMILIALE STRING,"
            + "NBENFANTSACHARGE STRING,"
            + "DEUXIEMEVOITURE STRING,"
            + "PRIMARY KEY (ID))"
        )
        self.execute_ddl(statement)

    def execute_ddl(self, statement):
        try:
            result = self.store.set(self.tabMarketing, (statement))
            self.display_result(result, statement)
        except Exception as e:
            print(f"Erreur lors de l'exécution de la commande DDL : {e}")

    def insert_a_marketing_row(self, age, sexe, taux, situationFamiliale, nbEnfantsAcharge, deuxiemeVoiture):
        try:
            marketingRow = {
                "ID": self.clientID,
                "AGE": age,
                "SEXE": sexe,
                "TAUX": taux,
                "SITUATIONFAMILIALE": situationFamiliale,
                "NBENFANTSACHARGE": nbEnfantsAcharge,
                "DEUXIEMEVOITURE": deuxiemeVoiture
            }
            result = self.store.set(self.tabMarketing, marketingRow)
            self.display_result(result, f"Insertion d'une ligne de marketing avec ID: {self.clientID}")
            self.clientID += 1
        except Exception as e:
            print(f"Erreur lors de l'insertion d'une ligne de marketing : {e}")

    def load_marketing_data_from_file(self):
        try:
            with open(self.pathToCSVFile, newline='', encoding='latin-1') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    age, sexe, taux, situationFamiliale, nbEnfantsAcharge, deuxiemeVoiture = row
                    self.insert_a_marketing_row(age, sexe, taux, situationFamiliale, nbEnfantsAcharge, deuxiemeVoiture)
        except Exception as e:
            print(f"Erreur lors du chargement des données marketing depuis le fichier : {e}")

if __name__ == "__main__":
    marketing_import_data = MarketingImportData()
    marketing_import_data.main()
