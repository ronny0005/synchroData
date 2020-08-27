import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

public class UpdateData {



    public static void main(String[] args){
        String databaseDestFile = "resource/databaseDest.csv";
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        ArrayList<String> list = DataBase.getInfoConnexion(databaseDestFile);

        String path = list.get(4);


        try {
            String dbURL = "jdbc:sqlserver://"+list.get(0)+";databaseName="+list.get(1);
            Properties properties = new Properties();
            properties.put("user", list.get(2));
            properties.put("password", list.get(3));
            Connection sqlCon = DriverManager.getConnection(dbURL, properties);

            sqlCon.setAutoCommit(true);
//            DocLigne.sendDataElement(sqlCon, path,list.get(1));

            if(list.get(7).equals("1"))
                Depot.sendDataElement(sqlCon, path,list.get(1));
            if(list.get(12).equals("1"))
                Famille.sendDataElement(sqlCon, path,list.get(1));
            if(list.get(6).equals("1"))
                Comptet.sendDataElement(sqlCon, path,list.get(1));
            if(list.get(8).equals("1"))
                Livraison.sendDataElement(sqlCon, path,list.get(1));
            if(list.get(5).equals("1"))
                Article.sendDataElement(sqlCon, path,list.get(1));
            if(list.get(9).equals("1")) {
                DocEntete.sendDataElement(sqlCon, path,list.get(1));
                DocRegl.sendDataElement(sqlCon, path,list.get(1));
            }
            if(list.get(10).equals("1"))
                DocLigne.sendDataElement(sqlCon, path,list.get(1));
            if(list.get(11).equals("1")) {
                Reglement.sendDataElement(sqlCon, path,list.get(1));
                ReglEch.sendDataElement(sqlCon, path,list.get(1));
            }

            if(list.get(13).equals("1")) {
                ArtStock.sendDataElement(sqlCon, path,list.get(1));
            }
            if(list.get(14).equals("1")) {
                EcritureC.sendDataElement(sqlCon, path,list.get(1));
            }
            if(list.get(15).equals("1")) {
                EcritureA.sendDataElement(sqlCon, path,list.get(1));
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
