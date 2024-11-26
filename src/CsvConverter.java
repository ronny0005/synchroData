import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class CsvConverter {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.s";
    private static Statement stmt;

    private static String escapeDoubleQuotes(String value) {
        return value.replaceAll("\"", "\"\"");
    }

    private static int writeHeaderLine(ResultSet result,FileWriter fileWriter) throws SQLException, IOException {
        // write header line containing column names
        ResultSetMetaData metaData = result.getMetaData();
        int numberOfColumns = metaData.getColumnCount();
        String headerLine = "";

        // exclude the first column which is the ID field
        for (int i = 1; i <= numberOfColumns; i++) {
            String columnName = metaData.getColumnName(i);
            headerLine = headerLine.concat(columnName).concat(";");
        }

        fileWriter.write(headerLine.substring(0, headerLine.length() - 1)+"\n");

        return numberOfColumns;
    }

    public static void writeOnFile(String fileName, String query, Connection sqlCon)
    {
        ResultSet result;
        FileWriter fileWriter;
        try {
            result = Table.executeQueryResult(sqlCon,query);

            if(result.isBeforeFirst()) {          //res.isBeforeFirst() is true if the cursor
                fileWriter = new FileWriter(fileName, StandardCharsets.ISO_8859_1);
                int columnCount = writeHeaderLine(result, fileWriter);
                while (result.next()) {
                    String line = "";

                    for (int i = 1; i <= columnCount; i++) {
                        Object valueObject = result.getObject(i);
                        String valueString = "";

                        if (valueObject != null) valueString = valueObject.toString();

                        if (valueObject instanceof String) {
                            valueString = "\"" + escapeDoubleQuotes(valueString) + "\"";
                        }

                        if (isDateValid(valueString)) {
                            valueString = "\"" + valueString.replace(".0", "") + "\"";
                        }

                        line = line.concat(valueString);

                        if (i != columnCount) {
                            line = line.concat(";");
                        }
                    }

                    fileWriter.write(line + "\n");
                    fileWriter.flush();
                }
                fileWriter.close();
            }
        } catch (SQLException | IOException throwables) {
            throwables.printStackTrace();
        }
    }

    public static boolean isDateValid(String date)
    {
        try {
            DateFormat df = new SimpleDateFormat(DATE_FORMAT);
            df.setLenient(false);
            df.parse(date);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    public static void importCsvToSqlTable(Connection sqlCon, String filePath, String tableName) {
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                throw new FileNotFoundException("Fichier introuvable : " + filePath);
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.ISO_8859_1));
            String line;
            String[] headers = null;
            List<String[]> dataRows = new ArrayList<>();
            int lineNumber = 0;

            while ((line = br.readLine()) != null) {
                String[] values = line.split(";");
                if (lineNumber == 0) {
                    headers = values; // Stocker les en-têtes
                } else {
                    dataRows.add(values); // Stocker les données
                }
                lineNumber++;
            }
            br.close();

            if (headers == null || dataRows.isEmpty()) {
                throw new IllegalArgumentException("Le fichier CSV est vide ou mal formaté.");
            }

            // Déterminer les types des colonnes
            String[] columnTypes = determineColumnTypes(headers.length, dataRows);

            // Créer dynamiquement la table
            createTable(sqlCon, tableName, headers, columnTypes);

            // Insérer les données
            insertData(sqlCon, tableName, headers, dataRows);

            System.out.println("Importation terminée avec succès dans la table : " + tableName);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Déterminer les types des colonnes
    private static String[] determineColumnTypes(int columnCount, List<String[]> dataRows) {
        String[] columnTypes = new String[columnCount];

        for (int col = 0; col < columnCount; col++) {
            boolean isInteger = true;
            boolean isFloat = true;
            boolean isDate = true;

            for (String[] row : dataRows) {
                if (row.length > col) {
                    String value = row[col].trim();
                    if (value.isEmpty()) continue;

                    if (isInteger && !value.matches("-?\\d+")) isInteger = false;
                    if (isFloat && !value.matches("-?\\d+(\\.\\d+)?")) isFloat = false;
                    if (isDate && !value.matches("\\d{4}-\\d{2}-\\d{2}.*")) isDate = false;

                    if (!isInteger && !isFloat && !isDate) break;
                }
            }

            // Attribuer un type basé sur les données
            if (isInteger) {
                columnTypes[col] = "INT";
            } else if (isFloat) {
                columnTypes[col] = "FLOAT";
            } else if (isDate) {
                columnTypes[col] = "DATETIME";
            } else {
                columnTypes[col] = "NVARCHAR(255)";
            }
        }

        return columnTypes;
    }
/*
    // Créer la table dans SQL Server
    private static void createTable(Connection sqlCon, String tableName, String[] headers, String[] columnTypes) throws SQLException {
        StringBuilder createTableQuery = new StringBuilder("CREATE TABLE " + tableName + " (");
        for (int i = 0; i < headers.length; i++) {
            createTableQuery.append(headers[i]).append(" ").append(columnTypes[i]).append(",");
        }
        createTableQuery.deleteCharAt(createTableQuery.length() - 1); // Supprimer la dernière virgule
        createTableQuery.append(");");

        try (Statement stmt = sqlCon.createStatement()) {
            stmt.execute("IF OBJECT_ID('" + tableName + "', 'U') IS NOT NULL DROP TABLE " + tableName + ";");
            stmt.execute(createTableQuery.toString());
        }

        System.out.println("Table créée : " + tableName);
    }
*/
    // Insérer les données dans la table
    private static void insertData(Connection sqlCon, String tableName, String[] headers, List<String[]> dataRows) throws SQLException {
        String insertSQL = "INSERT INTO " + tableName + " (" + String.join(",", headers) + ") VALUES (";
        insertSQL += String.join(",", "?".repeat(headers.length).split("")) + ");";

        try (PreparedStatement pstmt = sqlCon.prepareStatement(insertSQL)) {
            for (String[] row : dataRows) {
                for (int i = 0; i < headers.length; i++) {
                    if (i < row.length && !row[i].trim().isEmpty()) {
                        pstmt.setString(i + 1, row[i].trim());
                    } else {
                        pstmt.setNull(i + 1, java.sql.Types.NULL);
                    }
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }


    private static void executeBatch(Connection sqlCon, String sql, List<String[]> batch, int columnCount) throws SQLException {
        try (PreparedStatement pstmt = sqlCon.prepareStatement(sql)) {
            for (String[] values : batch) {
                for (int i = 0; i < columnCount; i++) {
                    String value = values[i].replace("\"", "").trim();
                    if (value.isEmpty()) {
                        pstmt.setNull(i + 1, java.sql.Types.NVARCHAR);
                    } else {
                        pstmt.setString(i + 1, value);
                    }
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }


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
                    String columnType = mapSqlTypeToCsvType(metaData.getColumnTypeName(i), metaData.getPrecision(i));
                    fileWriter.append(columnType);
                    if (i < columnCount) fileWriter.append(";");
                }
                fileWriter.append("\n");

                // Écriture des données
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        Object value = rs.getObject(i);
                        fileWriter.append(value != null ? value.toString() : "null");
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


    private static String mapSqlTypeToCsvType(String sqlType, int precision) {
        switch (sqlType.toUpperCase()) {
            case "INT":
            case "INTEGER": return "INTEGER";
            case "FLOAT":
            case "REAL": return "FLOAT";
            case "DECIMAL":
            case "NUMERIC": return "DECIMAL(" + precision + ")";
            case "CHAR":
            case "VARCHAR": return "VARCHAR(" + precision + ")";
            case "DATE":
            case "TIMESTAMP": return "DATE";
            case "BLOB":
            case "CLOB": return "BLOB";
            default: return "VARCHAR(255)";
        }
    }

    public static void importCsvAndCreateTable(Connection sqlCon, String csvPath, String tableName) {
        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
            String line;
            String[] headers = null;
            String[] types = null;
            int lineNumber = 0;

            while ((line = br.readLine()) != null) {
                if (lineNumber == 0) {
                    headers = line.split(";");
                } else if (lineNumber == 1) {
                    types = line.split(";");
                    createTable(sqlCon, tableName, headers, types);
                } else {
                    insertCsvRow(sqlCon, tableName, headers, line.split(";"));
                }
                lineNumber++;
            }

            System.out.println("Importation terminée avec succès dans la table : " + tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createTable(Connection sqlCon, String tableName, String[] headers, String[] types) throws SQLException {
        StringBuilder sql = new StringBuilder("DROP TABLE IF EXISTS " + tableName + ";CREATE TABLE " + tableName + " (");
        for (int i = 0; i < headers.length; i++) {
            sql.append(headers[i]).append(" ").append(types[i]).append(",");
        }
        sql.deleteCharAt(sql.length() - 1).append(");");

        try (Statement stmt = sqlCon.createStatement()) {
            stmt.executeUpdate(sql.toString());
            System.out.println("Table créée : " + tableName);
        }
    }

    private static void insertCsvRow(Connection sqlCon, String tableName, String[] headers, String[] values) throws SQLException {
        String placeholders = String.join(",", "?".repeat(headers.length).split(""));
        String sql = "INSERT INTO " + tableName + " (" + String.join(",", headers) + ") VALUES (" + placeholders + ")";

        try (PreparedStatement pstmt = sqlCon.prepareStatement(sql)) {
            for (int i = 0; i < values.length; i++) {
                String value = values[i].trim();
                if (value.equalsIgnoreCase("null") || value.isEmpty()) {
                    pstmt.setNull(i + 1, java.sql.Types.NULL);
                } else {
                    pstmt.setString(i + 1, value); // Vous pouvez ajuster ici selon les types
                }
            }
            pstmt.executeUpdate();
        }
    }


}
