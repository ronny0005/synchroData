import java.io.*;
import java.sql.*;

public class BCPExporter {

    public static void exportDataToBCP(String query, String filePath, String server, String database, String username, String password) {
        try {
            // Création d'un fichier de format BCP personnalisé
            String formatFilePath = filePath.replace(".bcp","") + ".fmt";

            // Créer le fichier de format BCP à l'aide de la commande BCP
            String createFormatFileCommand = String.format(
                    "bcp \"%s\" format nul -n -S %s -d %s -U %s -P %s -f \"%s\"",
                    query, server, database, username, password, formatFilePath
            );
            // Exécuter la commande pour générer le fichier de format
            Process process = Runtime.getRuntime().exec(createFormatFileCommand);

            // Lire la sortie de la commande de création du fichier de format
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            }

            // Attendre que la commande se termine
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                System.err.println("Erreur lors de la création du fichier de format BCP.");
                return;
            }

            // Exécuter la commande BCP avec le fichier de format généré
            String exportCommand = String.format(
                    "bcp \"%s\" queryout \"%s\" -f \"%s\" -S %s -d %s -U %s -P %s",
                    query, filePath, formatFilePath, server, database, username, password
            );
            // Exécuter la commande d'exportation
            process = Runtime.getRuntime().exec(exportCommand);

            // Lire la sortie de la commande d'exportation
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            }

            // Vérifier si la commande a réussi
            exitCode = process.waitFor();
            if (exitCode == 0) {
                System.out.println("Exportation réussie vers " + filePath);
            } else {
                System.err.println("Erreur lors de l'exécution de la commande BCP.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void importDataToBCP(String filePath, String tableName, Connection sqlCon) {
        try (Connection connection = sqlCon;
             Statement statement = connection.createStatement()) {

            // Commande BULK INSERT
            String bulkInsertQuery = "BULK INSERT " + tableName +
                    " FROM '" + filePath + "' " +
                    "WITH (" +
                    "DATAFILETYPE = 'native', " +
                    "FIELDTERMINATOR = ',', " +
                    "ROWTERMINATOR = '\\n', " +
                    "TABLOCK" +
                    ")";
            statement.execute(bulkInsertQuery);

            System.out.println("Importation réussie !");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Erreur lors de l'importation du fichier BCP : " + e.getMessage());
        }
    }


    public static void exportDataToBCPWithFormat(String query, String filePath, String formatFilePath,
                                                 String server, String database, String username, String password) {
        try {
            // Connexion à la base de données
            Connection connection = DriverManager.getConnection(
                    "jdbc:sqlserver://"+server+";databaseName="+database, username, password);

            // Créer la commande BCP pour l'exportation avec le fichier de format
            String bcpCommand = String.format(
                    "bcp \"%s\" queryout \"%s\" -f \"%s\" -S %s -d %s -U %s -P %s",
                    query, filePath, formatFilePath, server, database, username, password);

            // Créer le fichier de format
            createBCPFormatFile(connection, query, formatFilePath);

            // Exécuter la commande BCP
            Process process = Runtime.getRuntime().exec(bcpCommand);

            // Lire la sortie de la commande
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            }

            // Vérifier si la commande a réussi
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                System.out.println("Exportation réussie vers " + filePath);
            } else {
                System.err.println("Erreur lors de l'exécution de la commande BCP.");
            }

            // Fermer la connexion à la base de données
            connection.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Méthode pour générer le fichier de format BCP
    private static void createBCPFormatFile(Connection connection, String query, String formatFilePath) throws SQLException, IOException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query);

        // Créer le fichier de format BCP
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(formatFilePath))) {
            int columnCount = rs.getMetaData().getColumnCount();
            writer.write("10.0\n");  // Version BCP

            // Écrire les informations sur les colonnes et leur format
            for (int i = 1; i <= columnCount; i++) {
                String columnName = rs.getMetaData().getColumnName(i);
                String columnType = getColumnBCPType(rs.getMetaData().getColumnType(i));

                // Le format BCP contient l'index de la colonne, le type, et le nom de la colonne
                writer.write(i + " " + columnType + " 0 0 " + columnName + "\n");
            }
        }
    }

    // Méthode pour obtenir le type BCP à partir du type SQL
    private static String getColumnBCPType(int sqlType) {
        switch (sqlType) {
            case Types.INTEGER:
                return "int";
            case Types.FLOAT:
            case Types.REAL:
                return "float";
            case Types.VARCHAR:
                return "char";
            case Types.DATE:
                return "datetime";
            case Types.TIMESTAMP:
                return "datetime";
            // Ajoutez d'autres types si nécessaire
            default:
                return "char"; // Par défaut, utiliser 'char' pour la compatibilité
        }
    }


}
