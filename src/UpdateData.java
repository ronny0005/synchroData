import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class UpdateData {



    public static void main(String[] args){
        String databaseDestFile = "resource/databaseSource.json";
        if(args.length > 0)
            databaseDestFile = args[0];

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        JSONArray listObject = DataBase.getInfoConnexion(databaseDestFile);
        for(int i = 0 ; i< listObject.size() ; i++) {
            JSONObject list = (JSONObject) listObject.get(i);

            if(((String)list.get("reception")).equals("1")) {
                String path = ((String) list.get("path"));

                try {
                    String dbURL = "jdbc:sqlserver://" + ((String) list.get("servername")) + ";databaseName=" + ((String) list.get("database"));
                    Properties properties = new Properties();
                    properties.put("user", ((String) list.get("username")));
                    properties.put("password", ((String) list.get("password")));
                    Connection sqlCon = DriverManager.getConnection(dbURL, properties);
                    sqlCon.setAutoCommit(true);
                    System.out.println("--Démarrage Application--");
                    System.out.println("Base de données : " + ((String) list.get("database")) + "\n Chemin : " + path + "\n");
                    System.out.println("Serveur : " + ((String) list.get("servername")));
                    String database = (String) list.get("database");
//            DocLigne.sendDataElement(sqlCon, path,list.get(1));

                    if (((String) list.get("compteg")).equals("1")) {
                        System.out.println("--Chargement Compte Général--");
                        Compteg.sendDataElement(sqlCon, path, database);
                    }
                    if (((String) list.get("taxe")).equals("1")) {
                        System.out.println("--Chargement Taxe--");
                        Taxe.sendDataElement(sqlCon, path, database);
                    }
                    if (((String) list.get("collaborateur")).equals("1")) {
                        System.out.println("--Chargement Collaborateur--");
                        Collaborateur.sendDataElement(sqlCon, path, database);
                    }
                    if (((String) list.get("journaux")).equals("1")) {
                        System.out.println("--Chargement Journaux--");
                        FJournaux.sendDataElement(sqlCon, path, database);
                    }
                    if (((String) list.get("depot")).equals("1")) {
                        System.out.println("--Chargement Depot--");
                        Depot.sendDataElement(sqlCon, path, database);
                    }

                    if (((String) list.get("tiers")).equals("1")) {
                        System.out.println("--Chargement Compte Tiers--");
                        Comptet.sendDataElement(sqlCon, path, database);
                        System.out.println("--Chargement Livraison--");
                        Livraison.sendDataElement(sqlCon, path, database);
                    }

                    if (((String) list.get("article")).equals("1")) {
                        System.out.println("--Chargement Famille--");
                        Famille.sendDataElement(sqlCon, path, database);
                        System.out.println("--Chargement Article--");
                        Article.sendDataElement(sqlCon, path, database);
                    }

                    if (((String) list.get("entete")).equals("1")) {
                        System.out.println("--Chargement Entete--");
                        DocEntete.sendDataElement(sqlCon, path, database);
                        System.out.println("--Chargement DocRegl--");
                        DocRegl.sendDataElement(sqlCon, path, database);
                    }

                    if (((String) list.get("ligne")).equals("1")) {
                        System.out.println("--Chargement Ligne--");
                        DocLigne.sendDataElement(sqlCon, path, database);
                    }

                    if (((String) list.get("reglement")).equals("1")) {
                        System.out.println("--Chargement Reglement--");
                        Reglement.sendDataElement(sqlCon, path, database);
                        System.out.println("--Chargement ReglEch--");
                        ReglEch.sendDataElement(sqlCon, path, database);
                    }

                    if (((String) list.get("artstock")).equals("1")) {
                        System.out.println("--Chargement ArtStock--");
                        ArtStock.sendDataElement(sqlCon, path, database);
                    }

                    if (((String) list.get("ecriturec")).equals("1")) {
                        System.out.println("--Chargement JMouv--");
                        JMouv.sendDataElement(sqlCon, path, database);

                        System.out.println("--Chargement Ecriture Comptable--");
                        EcritureC.sendDataElement(sqlCon, path, database);
                    }

                    if (((String) list.get("ecriturea")).equals("1")) {
                        System.out.println("--Chargement Ecriture Analytique--");
                        EcritureA.sendDataElement(sqlCon, path, database);
                    }

                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }
}
