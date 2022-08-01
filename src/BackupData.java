import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class BackupData {

    public static void main(String[] args){
        String databaseSourceFile = "resource/databaseSource.json";
        if(args.length > 0)
            databaseSourceFile = args[0];
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        JSONArray listObject = DataBase.getInfoConnexion(databaseSourceFile);
        if (listObject != null) {
            for (Object o : listObject) {
                JSONObject list = (JSONObject) o;

                if (list.get("envoi").equals("1") && list.get("active").equals("1")) {
                    String path = (String) list.get("path");
                    try {
                        String dbURL = "jdbc:sqlserver://" + list.get("servername") + ";databaseName=" + list.get("database");
                        Properties properties = new Properties();
                        properties.put("user", list.get("username"));
                        properties.put("password", list.get("password"));
                        Connection sqlCon = DriverManager.getConnection(dbURL, properties);
                        sqlCon.setAutoCommit(true);

    //            ReglEch.getDataElement(sqlCon, path,list.get(1));
                        String database = (String) list.get("database");

                        if (list.get("taxe").equals("1")) {
                            System.out.println("--Sauvegarde taxe--");
                            Taxe.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()));
                        }
                        if (list.get("depot").equals("1")) {
                            System.out.println("--Sauvegarde depot--");
                            Depot.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()));
                        }
                        if (list.get("tiers").equals("1")) {
                            System.out.println("--Sauvegarde tiers--");
                            Comptet.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()));
                            System.out.println("--Sauvegarde livraison--");
                            Livraison.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()));
                        }
                        if (list.get("article").equals("1")) {
                            System.out.println("--Sauvegarde famille--");
                            Famille.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()));
                            System.out.println("--Sauvegarde article--");
                            Article.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()));
                        }
                        if (list.get("entete").equals("1")) {
                            System.out.println("--Sauvegarde Caisse--");
                            Caisse.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()));
                            System.out.println("--Sauvegarde entete--");
                            DocEntete.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()), (JSONObject) list.get("typedocument"));
                            System.out.println("--Sauvegarde DocRegl--");
                            DocRegl.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()), (JSONObject) list.get("typedocument"));
                        }
                        if (list.get("ligne").equals("1")) {
                            System.out.println("--Sauvegarde ligne--");
                            DocLigne.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()), (JSONObject) list.get("typedocument"));

                        }
                        if (list.get("reglement").equals("1")) {
                            System.out.println("--Sauvegarde ReglEch--");
                            ReglEch.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()), (JSONObject) list.get("typedocument"));
                            System.out.println("--Sauvegarde Reglement--");
                            Reglement.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()));
                        }
                        if ((list.get("artstock")).equals("1")) {
                            System.out.println("--Sauvegarde ArtStock--");
                            ArtStock.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()));
                        }
                        if ((list.get("ecriturec")).equals("1")) {
                            System.out.println("--Sauvegarde JMouv--");
                            JMouv.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()));
                            System.out.println("--Sauvegarde EcritureC--");
                            EcritureC.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()));
                        }
                        if ((list.get("ecriturea")).equals("1")) {
                            System.out.println("--Sauvegarde EcritureA--");
                            EcritureA.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()));
                        }
                        if ((list.get("compteg")).equals("1")) {
                            System.out.println("--Sauvegarde CompteG--");
                            Compteg.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()));
                        }
                        if ((list.get("journaux")).equals("1")) {
                            System.out.println("--Sauvegarde Journaux--");
                            FJournaux.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()));
                        }
                        if (list.get("collaborateur").equals("1")) {
                            System.out.println("--Sauvegarde Collaborateur--");
                            Collaborateur.getDataElement(sqlCon, path, database, simpleDateFormat.format(new Date()));
                        }
                        /*if (((String) list.get("caisse")).equals("1")) {
                        }*/

                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }
        }
    }
}
