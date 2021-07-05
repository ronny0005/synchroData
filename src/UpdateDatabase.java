import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

public class UpdateDatabase {

    public static void executeSQL(File f, Connection c) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(f));
        String sql = "", line;
        while ((line = br.readLine()) != null) sql += (line+"\n");

        try {
            Statement stmt = c.createStatement();
            stmt.execute(sql);
            stmt.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void main(String[] args){

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        String databaseSourceFile = "resource/databaseSource.csv";
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        ArrayList<String> list = DataBase.getInfoConnexion(databaseSourceFile);
        String path = list.get(4);
        try {
            String dbURL = "jdbc:sqlserver://"+list.get(0)+";databaseName="+list.get(1);
            Properties properties = new Properties();
            properties.put("user", list.get(2));
            properties.put("password", list.get(3));
            Connection sqlCon = DriverManager.getConnection(dbURL, properties);
            executeSQL(new File("resource/configListTable.sql"),sqlCon);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
