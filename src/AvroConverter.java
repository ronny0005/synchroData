import org.apache.avro.InvalidAvroMagicException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.List;

public class AvroConverter {



    // Method to map Avro types to SQL Server types
    private static String mapAvroTypeToSQLType(org.apache.avro.Schema schema) {
        org.apache.avro.Schema.Type avroType = schema.getType();
        if (avroType == org.apache.avro.Schema.Type.UNION) {
            // If it's a union, find the non-null type
            List<Schema> types = schema.getTypes();
            for (org.apache.avro.Schema type : types) {
                if (!type.getType().equals(org.apache.avro.Schema.Type.NULL)) {
                    avroType = type.getType();
                    break;
                }
            }
        }

        switch (avroType) {
            case INT:
                return "INT";
            case LONG:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "FLOAT";
            case BOOLEAN:
                return "BIT";
            case STRING:
                return "NVARCHAR(MAX)";
            case BYTES:
                return "VARBINARY(MAX)";
            default:
                return "NVARCHAR(MAX)";
        }
    }

    public static boolean isValidAvroFile(String filePath) {
        try (FileInputStream fis = new FileInputStream(filePath)) {
            byte[] magicBytes = new byte[4];
            if (fis.read(magicBytes) != 4) {
                return false; // Fichier trop petit pour être un fichier Avro valide
            }
            // Comparer les octets magiques avec ceux attendus pour Avro
            return (magicBytes[0] == 0x4F && magicBytes[1] == 0x62 &&
                    magicBytes[2] == 0x6A && magicBytes[3] == 0x01);
        } catch (IOException e) {
            // Gérer les erreurs liées à la lecture du fichier
            System.err.println("Erreur lors de la lecture du fichier: " + filePath);
            return false;
        }
    }


    // Method to create a SQL Server table from Avro schema
    private static void createTableFromSchema(Connection conn, org.apache.avro.Schema schema, String tableName) throws SQLException {
        StringBuilder createTableQuery = new StringBuilder("DROP TABLE IF EXISTS ").append(tableName).append("; CREATE TABLE ");
        createTableQuery.append(tableName).append(" (");

        for (org.apache.avro.Schema.Field field : schema.getFields()) {
            createTableQuery.append(field.name()).append(" ");
            createTableQuery.append(mapAvroTypeToSQLType(field.schema())).append(",");
        }

        // Remove the last comma
        createTableQuery.setLength(createTableQuery.length() - 1);
        createTableQuery.append(");");

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableQuery.toString());
        }
    }

    // Method to generate the SQL insert query from Avro schema
    private static String generateInsertQuery(org.apache.avro.Schema schema, String tableName) {
        StringBuilder insertQuery = new StringBuilder("INSERT INTO ");
        insertQuery.append(tableName).append(" (");

        StringBuilder valuesPart = new StringBuilder(" VALUES (");

        for (org.apache.avro.Schema.Field field : schema.getFields()) {
            insertQuery.append(field.name()).append(",");
            valuesPart.append("?,");
        }

        // Remove the last comma
        insertQuery.setLength(insertQuery.length() - 1);
        valuesPart.setLength(valuesPart.length() - 1);

        insertQuery.append(") ").append(valuesPart).append(");");
        return insertQuery.toString();
    }

    // Method to set parameters for PreparedStatement from GenericRecord
    private static void setPreparedStatementParameters(PreparedStatement preparedStatement, GenericRecord record) throws SQLException {
        int index = 1;
        for (org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
            Object value = record.get(field.name());
            org.apache.avro.Schema fieldSchema = field.schema();
            org.apache.avro.Schema.Type fieldType = fieldSchema.getType();

            // Handle union types by finding the non-null type
            if (fieldType == org.apache.avro.Schema.Type.UNION) {
                List<org.apache.avro.Schema> types = fieldSchema.getTypes();
                for (org.apache.avro.Schema type : types) {
                    if (!type.getType().equals(org.apache.avro.Schema.Type.NULL)) {
                        fieldType = type.getType();
                        break;
                    }
                }
            }

            if (value == null) {
                preparedStatement.setNull(index, getSqlType(fieldType));
            } else {
                switch (fieldType) {
                    case INT:
                        preparedStatement.setInt(index, (Integer) value);
                        break;
                    case LONG:
                        preparedStatement.setLong(index, (Long) value);
                        break;
                    case FLOAT:
                        preparedStatement.setFloat(index, (Float) value);
                        break;
                    case DOUBLE:
                        preparedStatement.setDouble(index, (Double) value);
                        break;
                    case BOOLEAN:
                        preparedStatement.setBoolean(index, (Boolean) value);
                        break;
                    case STRING:
                        preparedStatement.setString(index, value.toString());
                        break;
                    case BYTES:
                        preparedStatement.setBytes(index, (byte[]) value);
                        break;
                    default:
                        preparedStatement.setObject(index, value.toString());
                        break;
                }
            }
            index++;
        }
    }

    // Lire le fichier Avro et insérer les données dans la base de données
    public static void insertAvroDataToSqlServer(String avroFilePath, String tableName, Connection conn) {
        // Vérifier si le fichier est un fichier Avro valide avant de le lire
        if (!isValidAvroFile(avroFilePath)) {
            System.err.println("Le fichier n'est pas un fichier Avro valide: " + avroFilePath);

            String queryMessage = "INSERT INTO config.DB_Errors\n" +
                    "    VALUES\n" +
                    "  (SUSER_SNAME(),\n" +
                    "   ERROR_NUMBER(),\n" +
                    "   ERROR_STATE(),\n" +
                    "   ERROR_SEVERITY(),\n" +
                    "   ERROR_LINE(),\n" +
                    "   ERROR_PROCEDURE(),\n" +
                    "   '"+ avroFilePath +"',\n" +
                    "   '"+ avroFilePath +"',\n" +
                    "   '"+ tableName +"',\n" +
                    "   GETDATE());\n";
            Table.executeQuery(conn, queryMessage);

            return;
        }

        File avroFile = new File(avroFilePath);

        if (!avroFile.exists()) {
            System.err.println("File does not exist: " + avroFilePath);
            return;
        }

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();

        try (org.apache.avro.file.FileReader<GenericRecord> dataFileReader = DataFileReader.openReader(avroFile, datumReader)) {
            // Get Avro schema
            org.apache.avro.Schema schema = dataFileReader.getSchema();

            // Create table dynamically
            createTableFromSchema(conn, schema, tableName);

            // Prepare insert query
            String sqlInsertQuery = generateInsertQuery(schema, tableName);
            try (PreparedStatement preparedStatement = conn.prepareStatement(sqlInsertQuery)) {

                // Read each Avro record and insert into the database
                while (dataFileReader.hasNext()) {
                    GenericRecord record = dataFileReader.next();
                    setPreparedStatementParameters(preparedStatement, record);
                    preparedStatement.executeUpdate();
                }
            }
        } catch (InvalidAvroMagicException e) {
            System.err.println("Error: Not a valid Avro file. Path: " + avroFilePath);
        } catch (IOException e) {
            System.err.println("IO Exception while reading file: " + avroFilePath);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // Méthode principale pour écrire et valider un fichier Avro
    public static void writeToFileAvro(String fileName, String query, Connection sqlCon) {
        boolean isValid = false;
        int maxRetries = 3; // Limiter les tentatives pour éviter des boucles infinies
        int attempts = 0;

        try {
            // Vérifier si le ResultSet contient des données avant toute tentative
            if (!hasResultSetData(query, sqlCon)) {
                System.out.println("Le ResultSet est vide. Aucun fichier Avro ne sera créé.");
                return;
            }

            // Boucle pour tenter la création et validation du fichier Avro
            while (!isValid && attempts < maxRetries) {
                attempts++;
                try {
                    // Écrire le fichier Avro
                    writeAvroFile(fileName, query, sqlCon);

                    // Vérifier si le fichier Avro a été créé avant validation
                    File avroFile = new File(fileName);
                    if (avroFile.exists()) {
                        // Valider le fichier Avro
                        isValid = validateAvroFile(fileName);
                        if (!isValid) {
                            System.out.println("Le fichier Avro est invalide. Tentative de recréation...");
                            logError(sqlCon, "Le fichier Avro est invalide. Tentative de recréation..."+maxRetries, query, fileName);
                            // Supprimer le fichier corrompu avant de le recréer
                            avroFile.delete();
                        }
                    } else {
                        logError(sqlCon, "Aucun fichier à valider !", query, fileName);
                        System.out.println("Le fichier Avro n'a pas été créé. Aucun fichier à valider.");
                        break; // Sortir de la boucle si le fichier n'est pas créé
                    }
                } catch (Exception e) {
                    logError(sqlCon, e.getMessage(), query, fileName);
                    System.err.println("Erreur lors de la création ou de la validation du fichier Avro : " + e.getMessage());
                }
            }

            if (!isValid) {
                System.err.println("Impossible de créer un fichier Avro valide après " + maxRetries + " tentatives.");
                logError(sqlCon, "Échec complet après " + maxRetries + " tentatives", query, fileName);
            } else {
                System.out.println("Fichier Avro créé et validé avec succès !");
            }
        } catch (Exception e) {
            logError(sqlCon, e.getMessage(), query, fileName);
            System.err.println("Erreur critique lors de l'exécution : " + e.getMessage());
        }
    }

    // Vérifier si le ResultSet contient des données
    private static boolean hasResultSetData(String query, Connection sqlCon) throws SQLException {
        try (Statement statement = sqlCon.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            return resultSet.isBeforeFirst(); // Retourne true si des lignes existent
        }
    }

    // Enregistrer les erreurs dans la base de données
    public static void logError(Connection sqlCon, String errorMessage, String query, String fileName) {
        String queryMessage = "INSERT INTO config.DB_Errors\n" +
                "    VALUES\n" +
                "  (SUSER_SNAME(),\n" +
                "   ERROR_NUMBER(),\n" +
                "   ERROR_STATE(),\n" +
                "   ERROR_SEVERITY(),\n" +
                "   ERROR_LINE(),\n" +
                "   ERROR_PROCEDURE(),\n" +
                "   '"+ errorMessage.replace("'", "''") +"',\n" +
                "   '"+ fileName +"',\n" +
                "   NULL,\n" +
                "   GETDATE());\n";
        Table.executeQuery(sqlCon, queryMessage);
    }
    // Méthode pour écrire le fichier Avro
    private static void writeAvroFile(String fileName, String query, Connection sqlCon) throws Exception {
        try (Statement statement = sqlCon.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            // Vérifier si le ResultSet contient des lignes
            if (!resultSet.isBeforeFirst()) {
                // Le ResultSet est vide, ne pas créer de fichier
                System.out.println("Le ResultSet est vide. Aucun fichier Avro n'a été créé.");
                return;
            }

            // Récupérer les métadonnées pour construire le schéma Avro
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            StringBuilder schemaBuilder = new StringBuilder();
            schemaBuilder.append("{\"type\":\"record\",\"name\":\"ResultSetRecord\",\"fields\":[");

            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                String columnType = mapSQLTypeToAvroType(metaData.getColumnTypeName(i));
                schemaBuilder.append("{\"name\":\"").append(columnName)
                        .append("\",\"type\":").append(columnType)
                        .append("}");
                if (i < columnCount) {
                    schemaBuilder.append(",");
                }
            }
            schemaBuilder.append("]}");

            org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(schemaBuilder.toString());

            // Créer un Avro DataFileWriter
            File file = new File(fileName);
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>());
            dataFileWriter.create(schema, file);

            // Ecrire les résultats dans le fichier Avro
            while (resultSet.next()) {
                GenericRecord record = new GenericData.Record(schema);
                for (int i = 1; i <= columnCount; i++) {
                    Object value = resultSet.getObject(i);
                    String columnName = metaData.getColumnName(i);

                    if (value == null) {
                        record.put(columnName, null);
                    } else {
                        // Gérer les différents types
                        if (value instanceof BigDecimal) {
                            // Convertir BigDecimal en chaîne pour éviter les erreurs de type
                            record.put(columnName, value.toString());
                        } else if (value instanceof Short) {
                            // Convertir Short en int
                            record.put(columnName, ((Short) value).intValue());
                        } else if (value instanceof Number) {
                            // Gérer les types numériques simples
                            record.put(columnName, value);
                        } else {
                            // Convertir les autres types en chaîne
                            record.put(columnName, value.toString());
                        }
                    }
                }
                dataFileWriter.append(record);
            }

            // Fermer le writer
            dataFileWriter.close();
        }
    }



    // Méthode pour valider un fichier Avro
    private static boolean validateAvroFile(String fileName) {
        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(new File(fileName), new GenericDatumReader<>())) {
            while (reader.hasNext()) {
                reader.next();
            }
            return true; // Le fichier est valide
        } catch (Exception e) {
            System.err.println("Erreur de validation du fichier Avro : " + e.getMessage());
            return false;
        }
    }

    // Fonction de mappage SQL -> Avro
    private static String mapSQLTypeToAvroType(String sqlType) {
        switch (sqlType.toLowerCase()) {
            case "int":
            case "integer":
                return "[\"null\", \"int\"]";
            case "bigint":
                return "[\"null\", \"long\"]";
            case "float":
            case "real":
                return "[\"null\", \"float\"]";
            case "double":
            case "numeric":
            case "decimal":
                return "[\"null\", \"string\"]";
            //return "[\"null\", \"double\"]";
            case "bit":
            case "boolean":
                return "[\"null\", \"boolean\"]";
            case "char":
            case "varchar":
            case "nvarchar":
            case "text":
                return "[\"null\", \"string\"]";
            case "date":
            case "time":
            case "timestamp":
                return "[\"null\", {\"type\": \"string\", \"logicalType\": \"timestamp-millis\"}]";
            case "smallint":
                return "[\"null\", \"int\"]";
            case "tinyint":
                return "[\"null\", \"int\"]";
            default:
                return "[\"null\", \"string\"]";
        }
    }
    // Helper method to get SQL type from Avro schema type
    public static int getSqlType(org.apache.avro.Schema.Type avroType) {
        switch (avroType) {
            case INT:
                return java.sql.Types.INTEGER;
            case LONG:
                return java.sql.Types.BIGINT;
            case FLOAT:
                return java.sql.Types.FLOAT;
            case DOUBLE:
                return java.sql.Types.DOUBLE;
            case BOOLEAN:
                return java.sql.Types.BIT;
            case STRING:
                return java.sql.Types.NVARCHAR;
            case BYTES:
                return java.sql.Types.VARBINARY;
            default:
                return java.sql.Types.VARCHAR;
        }
    }
}
