import org.json.simple.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

public class BackupDataFilterAgency {

    public static void main(String[] args){

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        String databaseSourceFile = "resource/databaseSource.json";
        String agency ="YDE";

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        JSONObject list = DataBase.getInfoConnexion(databaseSourceFile);
        String path = ((String)list.get("path"));
        try {
            String dbURL = "jdbc:sqlserver://"+list.get(0)+";databaseName="+list.get(1);
            Properties properties = new Properties();
            properties.put("user", list.get(2));
            properties.put("password", list.get(3));
            Connection sqlCon = DriverManager.getConnection(dbURL, properties);

            sqlCon.setAutoCommit(true);
//            ReglEch.getDataElement(sqlCon, path,list.get(1));

            String database = ((String)list.get("database"));
            if(((String)list.get("depot")).equals("1"))
                Depot.getDataElementFilterAgency(sqlCon, path,database,simpleDateFormat.format(new Date()),agency);
            if(((String)list.get("famille")).equals("1"))
                Famille.getDataElement(sqlCon, path,database,simpleDateFormat.format(new Date()));
            if(((String)list.get("tiers")).equals("1"))
                Comptet.getDataElement(sqlCon, path,database,simpleDateFormat.format(new Date()));
            if(((String)list.get("livraison")).equals("1"))
                Livraison.getDataElement(sqlCon, path,database,simpleDateFormat.format(new Date()));
            if(((String)list.get("article")).equals("1"))
                Article.getDataElementFilterAgency(sqlCon, path,database,simpleDateFormat.format(new Date()),agency);
            if(((String)list.get("entete")).equals("1")) {
                DocEntete.getDataElementFilterAgency(sqlCon, path, database,simpleDateFormat.format(new Date()),agency);
                DocRegl.getDataElementFilterAgency(sqlCon, path, database,simpleDateFormat.format(new Date()),agency);
            }
            if(((String)list.get("ligne")).equals("1"))
                DocLigne.getDataElementFilterAgency(sqlCon, path,database,simpleDateFormat.format(new Date()),agency);
            if(((String)list.get("reglement")).equals("1")) {
                ReglEch.getDataElementFilterAgency(sqlCon, path,database,simpleDateFormat.format(new Date()),agency);
                Reglement.getDataElementFilterAgency(sqlCon, path, database,simpleDateFormat.format(new Date()),agency);
            }
            if(((String)list.get("artstock")).equals("1")) {
                ArtStock.getDataElementFilterAgency(sqlCon, path,database,simpleDateFormat.format(new Date()),agency);
            }
            if(((String)list.get("ecriturec")).equals("1")) {
                JMouv.getDataElement(sqlCon, path,database,simpleDateFormat.format(new Date()));
                EcritureC.getDataElement(sqlCon, path,database,simpleDateFormat.format(new Date()));
            }
            if(((String)list.get("ecriturea")).equals("1")) {
                EcritureA.getDataElement(sqlCon, path,database,simpleDateFormat.format(new Date()));
            }
            if(((String)list.get("compteg")).equals("1")) {
                Compteg.getDataElement(sqlCon, path,database,simpleDateFormat.format(new Date()));
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
