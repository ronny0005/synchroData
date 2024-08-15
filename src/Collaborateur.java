import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class Collaborateur extends Table {

    public static String file = "collaborateur_";
    public static String tableName = "F_COLLABORATEUR";
    public static String configList = "listCollaborateur";

    public static String insert(String filename)
    {
        return  "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                " IF OBJECT_ID('F_COLLABORATEUR_DEST') IS NOT NULL\n"+
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
                "   'Insert '+ ' "+filename+"',\n" +
                "   'F_COLLABORATEUR',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }

    public static void sendDataElement(Connection sqlCon, String path,int unibase)
    {
        File dir = new File(path);
        FilenameFilter filter = (dir1, name) -> name.startsWith(file);
        String[] children = dir.list(filter);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                readOnFile(path, "deleteList" + filename, tableName + "_SUPPR", sqlCon);
                executeQuery(sqlCon, updateTableDest("CO_No", "'CO_No','CO_Nom'", tableName, tableName + "_DEST", filename,unibase));
                sendData(sqlCon, path, filename, insert(filename));

                deleteTempTable(sqlCon, tableName + "_DEST");
                deleteCollaborateur(sqlCon, path, filename);
            }
        }
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"CO_No");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,database,true), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

    public static void deleteCollaborateur(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_COLLABORATEUR \n" +
                " WHERE CO_No IN(SELECT CO_No FROM F_COLLABORATEUR_SUPPR) \n" +
                " \n" +
                " IF OBJECT_ID('F_COLLABORATEUR_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_COLLABORATEUR_SUPPR \n";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }
}
