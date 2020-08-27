import java.io.File;
import java.sql.Connection;

public class Collaborateur extends Table {

    public static String file = "collaborateur.csv";
    public static String tableName = "F_COLLABORATEUR";
    public static String configList = "listCollaborateur";

    public static String insert()
    {
        return  "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                "INSERT INTO F_COLLABORATEUR (\n" +
                "[CO_No],[CO_Nom],[CO_Prenom],[CO_Fonction],[CO_Adresse],[CO_Complement],[CO_CodePostal],[CO_Ville],[CO_CodeRegion]\n" +
                "      ,[CO_Pays],[CO_Service],[CO_Vendeur],[CO_Caissier],[CO_DateCreation],[CO_Acheteur],[CO_Telephone],[CO_Telecopie]\n" +
                "      ,[CO_EMail],[CO_Receptionnaire],[PROT_No],[CO_TelPortable],[CO_ChargeRecouvr],[cbProt],[cbCreateur]\n" +
                "      ,[cbModification],[cbReplication],[cbFlag])\n" +
                "            \n" +
                "SELECT dest.[CO_No],[CO_Nom],[CO_Prenom],[CO_Fonction],[CO_Adresse],[CO_Complement],[CO_CodePostal],[CO_Ville],[CO_CodeRegion]\n" +
                "      ,[CO_Pays],[CO_Service],[CO_Vendeur],[CO_Caissier],[CO_DateCreation],[CO_Acheteur],[CO_Telephone],[CO_Telecopie]\n" +
                "      ,[CO_EMail],[CO_Receptionnaire],[PROT_No],[CO_TelPortable],[CO_ChargeRecouvr],[cbProt],[cbCreateur]\n" +
                "      ,[cbModification],[cbReplication],[cbFlag]\n" +
                "FROM F_COLLABORATEUR_DEST dest\n" +
                "LEFT JOIN (SELECT CO_No FROM F_COLLABORATEUR) src\n" +
                "\tON\tdest.CO_No = src.CO_No\n" +
                "WHERE src.CO_No IS NULL\n" +
                "            \n" +
                "IF OBJECT_ID('F_COLLABORATEUR_DEST') IS NOT NULL \n" +
                "DROP TABLE F_COLLABORATEUR_DEST;\n" +
                " END TRY\n" +
                " BEGIN CATCH \n" +
                "INSERT INTO config.DB_Errors\n" +
                "    VALUES\n" +
                "  (SUSER_SNAME(),\n" +
                "   ERROR_NUMBER(),\n" +
                "   ERROR_STATE(),\n" +
                "   ERROR_SEVERITY(),\n" +
                "   ERROR_LINE(),\n" +
                "   ERROR_PROCEDURE(),\n" +
                "   ERROR_MESSAGE(),\n" +
                "   'insert',\n" +
                "   'F_COLLABORATEUR',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }

    public static void sendDataElement(Connection sqlCon, String path,String database)
    {

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "CO_No","'CO_No'",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,insert());

        deleteTempTable(sqlCon,tableName);
        deleteCollaborateur(sqlCon, path);
    }

    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"CO_No");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList,database);
    }

    public static void deleteCollaborateur(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_COLLABORATEUR \n" +
                " WHERE CO_No IN(SELECT CO_No FROM F_COLLABORATEUR_SUPPR) \n" +
                " \n" +
                " IF OBJECT_ID('F_COLLABORATEUR_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_COLLABORATEUR_SUPPR \n";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }
}
