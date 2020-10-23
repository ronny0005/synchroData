import java.io.*;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

public class BackupData {

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

            sqlCon.setAutoCommit(true);
//            ReglEch.getDataElement(sqlCon, path,list.get(1));

            if(list.get(7).equals("1"))
                Depot.getDataElement(sqlCon, path,list.get(1),simpleDateFormat.format(new Date()));
            if(list.get(12).equals("1"))
                Famille.getDataElement(sqlCon, path,list.get(1),simpleDateFormat.format(new Date()));
            if(list.get(6).equals("1"))
                Comptet.getDataElement(sqlCon, path,list.get(1),simpleDateFormat.format(new Date()));
            if(list.get(8).equals("1"))
                Livraison.getDataElement(sqlCon, path,list.get(1),simpleDateFormat.format(new Date()));
            if(list.get(5).equals("1"))
                Article.getDataElement(sqlCon, path,list.get(1),simpleDateFormat.format(new Date()));
            if(list.get(9).equals("1")) {
                DocEntete.getDataElement(sqlCon, path, list.get(1),simpleDateFormat.format(new Date()));
                DocRegl.getDataElement(sqlCon, path, list.get(1),simpleDateFormat.format(new Date()));
            }
            if(list.get(10).equals("1"))
                DocLigne.getDataElement(sqlCon, path,list.get(1),simpleDateFormat.format(new Date()));
            if(list.get(11).equals("1")) {
                ReglEch.getDataElement(sqlCon, path,list.get(1),simpleDateFormat.format(new Date()));
                Reglement.getDataElement(sqlCon, path, list.get(1),simpleDateFormat.format(new Date()));
            }
            if(list.get(13).equals("1")) {
                ArtStock.getDataElement(sqlCon, path,list.get(1),simpleDateFormat.format(new Date()));
            }
            if(list.get(14).equals("1")) {
                EcritureC.getDataElement(sqlCon, path,list.get(1),simpleDateFormat.format(new Date()));
            }
            if(list.get(15).equals("1")) {
                EcritureA.getDataElement(sqlCon, path,list.get(1),simpleDateFormat.format(new Date()));
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
