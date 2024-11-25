import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.io.BufferedReader;
import java.io.FileReader;
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

    public static void importCSVToSQL(String csvFilePath, String tableName, Connection sqlConnection) {
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line;

            // Lire la première ligne pour les en-têtes de colonnes
            String headerLine = br.readLine();
            if (headerLine == null) {
                throw new IOException("Le fichier CSV est vide !");
            }

            String[] headers = headerLine.split(";");
            List<String> columnDefinitions = new ArrayList<>();

            // Lire la deuxième ligne pour détecter les types de données
            String dataLine = br.readLine();
            String[] firstRow = dataLine.split(";");

            // Déterminer les types de colonnes
            for (int i = 0; i < headers.length; i++) {
                String type = detectSQLType(firstRow[i]);
                columnDefinitions.add(headers[i] + " " + type);
            }

            // Générer la requête CREATE TABLE
            String createTableSQL = "CREATE TABLE " + tableName + " (\n" +
                    String.join(",\n", columnDefinitions) +
                    "\n);";

            // Exécuter la création de table
            try (Statement stmt = sqlConnection.createStatement()) {
                stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
                stmt.executeUpdate(createTableSQL);
                System.out.println("Table " + tableName + " créée avec succès !");
            }

            // Préparer l'insertion des données
            String insertSQL = generateInsertSQL(tableName, headers);
            try (PreparedStatement pstmt = sqlConnection.prepareStatement(insertSQL)) {
                // Insérer les lignes restantes
                do {
                    String[] values = dataLine.split(";");
                    for (int i = 0; i < values.length; i++) {
                        pstmt.setString(i + 1, values[i]);
                    }
                    pstmt.addBatch();
                } while ((dataLine = br.readLine()) != null);

                // Exécuter les insertions
                pstmt.executeBatch();
                System.out.println("Données insérées dans la table " + tableName);
            }

        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    // Détecter le type SQL en fonction de la valeur
    private static String detectSQLType(String value) {
        try {
            Integer.parseInt(value);
            return "INT";
        } catch (NumberFormatException e1) {
            try {
                Double.parseDouble(value);
                return "FLOAT";
            } catch (NumberFormatException e2) {
                return "VARCHAR(255)";
            }
        }
    }
    // Générer la requête INSERT INTO
    private static String generateInsertSQL(String tableName, String[] headers) {
        String columns = String.join(",", headers);

        // Créer une liste de "?" pour les placeholders en fonction du nombre de colonnes
        String placeholders = String.join(",", new String[headers.length].replace(null, "?"));

        return "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + placeholders + ")";
    }


}
