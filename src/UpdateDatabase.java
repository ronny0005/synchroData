import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class UpdateDatabase {

    public static void executeSQL(File f, Connection c) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(f));
        StringBuilder sql = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) sql.append(line).append("\n");

        try {
            Statement stmt = c.createStatement();
            stmt.execute(sql.toString());
            stmt.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void main(String[] args) {

        String databaseSourceFile = "resource/databaseSource.json";
        if (args.length > 0)
            databaseSourceFile = args[0];

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        JSONArray listObject = DataBase.getInfoConnexion(databaseSourceFile);
        if (listObject != null) {
            for (Object o : listObject) {
                JSONObject list = (JSONObject) o;
                if (list.get("reception").equals("1") && list.get("active").equals("1")) {
                    try {
                        String dbURL = "jdbc:sqlserver://" + list.get("servername") + ";databaseName=" + list.get("database");
                        Properties properties = new Properties();
                        properties.put("user", list.get("username"));
                        properties.put("password", list.get("password"));
                        Connection sqlCon = DriverManager.getConnection(dbURL, properties);
                        executeSQL(new File("resource/configListTable.sql"), sqlCon);
                    } catch (Exception throwables) {
                        throwables.printStackTrace();
                    }
                }
            }
        }
    }
}
