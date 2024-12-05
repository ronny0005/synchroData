import java.io.*;
import java.sql.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Utility class for converting database query results to CSV format and importing CSV data to a database.
 */
public class CsvConverter {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.s";
    private static Statement stmt;

    public static void exportToCsvWithTypes(String filePath, String query, Connection sqlCon) {
        try (Statement stmt = sqlCon.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            if (!rs.isBeforeFirst()) { // Vérifie si le ResultSet contient des lignes
                System.out.println("Aucun résultat trouvé pour la requête : " + filePath);
                return;
            }

            try (FileWriter fileWriter = new FileWriter(filePath)) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                // Écriture des noms des colonnes
                for (int i = 1; i <= columnCount; i++) {
                    fileWriter.append(metaData.getColumnName(i));
                    if (i < columnCount) fileWriter.append(";");
                }
                fileWriter.append("\n");

                // Écriture des types des colonnes
                for (int i = 1; i <= columnCount; i++) {
                    String columnType = mapSqlTypeToCsvType(
                            metaData.getColumnTypeName(i),  // Type SQL
                            metaData.getPrecision(i),      // Précision
                            metaData.getScale(i)           // Échelle
                    );
                    fileWriter.append(columnType);
                    if (i < columnCount) fileWriter.append(";");
                }
                fileWriter.append("\n");


                // Écriture des données
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        Object value = rs.getObject(i);
                        if (value != null) {
                            // Remplace les ";" par "," dans les valeurs texte
                            String stringValue = value.toString().replace(";", ",");
                            fileWriter.append(stringValue);
                        } else {
                            // Vérifie si la colonne est de type chaîne vide ""
                            String stringValue = rs.getString(i);
                            if (stringValue != null && stringValue.isEmpty()) {
                                fileWriter.append("");
                            } else {
                                fileWriter.append("null");
                            }
                        }
                        if (i < columnCount) fileWriter.append(";");
                    }
                    fileWriter.append("\n");
                }

                fileWriter.flush();
                System.out.println("Exportation terminée : " + filePath);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static String mapSqlTypeToCsvType(String sqlType, int precision, int scale) {
        switch (sqlType.toUpperCase()) {
            case "INT":
            case "INTEGER": return "INTEGER";
            case "FLOAT":
            case "REAL": return "FLOAT";
            case "DECIMAL":
            case "NUMERIC": return "DECIMAL(" + precision + "," + scale + ")";
            case "CHAR":
            case "VARCHAR": return "VARCHAR(" + precision + ")";
            case "DATE":
            case "TIMESTAMP": return "DATE";
            case "BLOB":
            case "CLOB": return "BLOB";
            default: return "VARCHAR(255)";
        }
    }

    private static int[] convertToSqlTypes(String[] types) {
        int[] sqlTypes = new int[types.length];

        for (int i = 0; i < types.length; i++) {
            String type = types[i].toUpperCase().trim();

            if (type.startsWith("VARCHAR")) {
                sqlTypes[i] = java.sql.Types.VARCHAR;
            } else if (type.startsWith("DECIMAL")) {
                sqlTypes[i] = java.sql.Types.DECIMAL;
            } else if (type.equals("INTEGER")) {
                sqlTypes[i] = java.sql.Types.INTEGER;
            } else if (type.equals("FLOAT")) {
                sqlTypes[i] = java.sql.Types.FLOAT;
            } else {
                sqlTypes[i] = java.sql.Types.VARCHAR;
            }
        }

        return sqlTypes;
    }

    public static void importCsvAndCreateTable(Connection sqlCon, String csvPath, String tableName) {
        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
            String line;
            String[] headers = null;  // Contiendra les noms de colonnes
            String[] types = null;    // Contiendra les types SQL
            int lineNumber = 0;

            while ((line = br.readLine()) != null) {
                if (lineNumber == 0) {
                    // Première ligne : Lire les noms des colonnes
                    headers = line.split(";");
                } else if (lineNumber == 1) {
                    // Deuxième ligne : Lire les types SQL
                    types = line.split(";");
                    // Créer dynamiquement la table avec les colonnes et types
                    createTable(sqlCon, tableName, headers, types);
                } else {
                    // Insérer les données dans la table
                    int[] columnTypes = convertToSqlTypes(types); // Convertir les types SQL pour l'insertion
                    insertCsvRow(sqlCon, tableName, headers, line.split(";"), columnTypes);
                }
                lineNumber++;
            }

            System.out.println("Importation terminée avec succès dans la table : " + tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void createTable(Connection sqlCon, String tableName, String[] headers, String[] types) throws SQLException {
        StringBuilder createQuery = new StringBuilder("DROP TABLE IF EXISTS "+tableName+"; CREATE TABLE " + tableName + " (");

        for (int i = 0; i < headers.length; i++) {
            createQuery.append(headers[i]).append(" ").append(types[i]); // Associe chaque colonne avec son type
            if (i < headers.length - 1) {
                createQuery.append(", ");
            }
        }
        createQuery.append(")");

        try (Statement stmt = sqlCon.createStatement()) {
            stmt.execute(createQuery.toString());
        }

        System.out.println("Table créée avec succès : " + tableName);
    }


    private static void insertCsvRow(Connection sqlCon, String tableName, String[] headers, String[] values, int[] columnTypes) throws SQLException {
        String placeholders = String.join(",", "?".repeat(headers.length).split(""));
        String sql = "INSERT INTO " + tableName + " (" + String.join(",", headers) + ") VALUES (" + placeholders + ")";

        try (PreparedStatement pstmt = sqlCon.prepareStatement(sql)) {
            for (int i = 0; i < values.length; i++) {
                String value = values[i].trim();
                if (value.equalsIgnoreCase("null") || value.isEmpty()) {
                    pstmt.setNull(i + 1, columnTypes[i]); // Insérer une valeur NULL si nécessaire
                } else {
                    // Insérer selon le type
                    switch (columnTypes[i]) {
                        case java.sql.Types.INTEGER:
                            pstmt.setInt(i + 1, Integer.parseInt(value));
                            break;
                        case java.sql.Types.FLOAT:
                            pstmt.setFloat(i + 1, Float.parseFloat(value));
                            break;
                        case java.sql.Types.DECIMAL:
                            pstmt.setBigDecimal(i + 1, new java.math.BigDecimal(value));
                            break;
                        case java.sql.Types.VARCHAR:
                            pstmt.setString(i + 1, value);
                            break;
                        default:
                            pstmt.setString(i + 1, value); // Par défaut, insérer comme une chaîne
                            break;
                    }
                }
            }
            pstmt.executeUpdate();
        }
    }





}
