package marketing;

import oracle.kv.KVStore;
import java.util.*;
import oracle.kv.*;
import oracle.kv.StatementResult;
import oracle.kv.table.*;
import java.lang.Integer;
import java.io.*;

public class MarketingImportData {

    private final KVStore store;
    private final String TABLE_MARKETING = "MARKETING";
    private final String CSV_FILE_PATH = "../../vagrant/projet/data/Marketing.csv";
    private int clientId = 1;

    public static void main(String[] args) {
        try {
            MarketingImportData mark = new MarketingImportData(args);
            mark.initMarketingTablesAndData();
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    MarketingImportData(String[] argv) {
        String storeName = "kvstore";
        String hostName = "localhost";
        String hostPort = "5000";
        store = KVStoreFactory.getStore(new KVStoreConfig(storeName, hostName + ":" + hostPort));
    }

    private void displayResult(StatementResult result, String statement) {
        System.out.println("===========================");
        if (result.isSuccessful()) {
            System.out.println("Statement was successful:\n\t" + statement);
            System.out.println("Results:\n\t" + result.getInfo());
        } else if (result.isCancelled()) {
            System.out.println("Statement was cancelled:\n\t" + statement);
        } else {
            if (result.isDone()) {
                System.out.println("Statement failed:\n\t" + statement);
                System.out.println("Problem:\n\t" + result.getErrorMessage());
            } else {
                System.out.println("Statement in progress:\n\t" + statement);
                System.out.println("Status:\n\t" + result.getInfo());
            }
        }
    }

    public void initMarketingTablesAndData() {
        dropTableMarketing();
        createTableMarketing();
        loadMarketingDataFromFile(CSV_FILE_PATH);
    }

    public void dropTableMarketing() {
        String statement = "DROP TABLE " + TABLE_MARKETING;
        executeDDL(statement);
    }

    public void createTableMarketing() {
        String statement = "CREATE TABLE " + TABLE_MARKETING + " (" +
                           "ID INTEGER," +
                           "AGE STRING," +
                           "SEXE STRING," +
                           "TAUX STRING," +
                           "SITUATIONFAMILIALE STRING," +
                           "NBENFANTSACHARGE STRING," +
                           "DEUXIEMEVOITURE STRING," +
                           "PRIMARY KEY (ID))";
        executeDDL(statement);
    }

    public void executeDDL(String statement) {
        TableAPI tableAPI = store.getTableAPI();
        System.out.println("****** Executing DDL: " + statement + " ********");
        try {
            StatementResult result = store.executeSync(statement);
            displayResult(result, statement);
        } catch (IllegalArgumentException | FaultException e) {
            System.out.println("Error executing statement:\n" + e.getMessage());
        }
    }

    private void insertMarketingRow(String age, String sexe, String taux, String situationFamiliale, String nbEnfantsAcharge, String deuxiemeVoiture) {
        System.out.println("***** Inserting Row in Marketing Table *****");

        try {
            TableAPI tableAPI = store.getTableAPI();
            Table marketingTable = tableAPI.getTable(TABLE_MARKETING);
            Row marketingRow = marketingTable.createRow();

            marketingRow.put("ID", clientId++);
            marketingRow.put("age", age);
            marketingRow.put("sexe", sexe);
            marketingRow.put("taux", taux);
            marketingRow.put("situationFamiliale", situationFamiliale);
            marketingRow.put("nbEnfantsAcharge", nbEnfantsAcharge);
            marketingRow.put("deuxiemeVoiture", deuxiemeVoiture);

            tableAPI.put(marketingRow, null, null);
        } catch (IllegalArgumentException | FaultException e) {
            System.out.println("Error inserting row:\n" + e.getMessage());
        }
    }

    void loadMarketingDataFromFile(String marketingDataFileName) {
        System.out.println("***** Loading Data From File: " + marketingDataFileName + " *****");

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(marketingDataFileName)))) {

            br.readLine();

            String line;
            while ((line = br.readLine()) != null) {
                StringTokenizer tokenizer = new StringTokenizer(line, ",");
                List<String> marketingRecord = new ArrayList<>();
                while (tokenizer.hasMoreTokens()) {
                    marketingRecord.add(tokenizer.nextToken());
                }

                insertMarketingRow(marketingRecord.get(0), marketingRecord.get(1), marketingRecord.get(2),
                                   marketingRecord.get(3), marketingRecord.get(4), marketingRecord.get(5));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}