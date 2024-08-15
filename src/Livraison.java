import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class Livraison extends Table {

    public static String file ="livraison_";
    public static String tableName = "F_LIVRAISON";
    public static String configList = "listLivraison";

    public static String insert(String filename)
    {
        return  "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +

                " IF OBJECT_ID('F_LIVRAISON_DEST') IS NOT NULL\n"+
                "INSERT INTO F_LIVRAISON ( [LI_No],[CT_Num],[LI_Intitule],[LI_Adresse],[LI_Complement],[LI_CodePostal],[LI_Ville]\n" +
                ",[LI_CodeRegion],[LI_Pays],[LI_Contact],[N_Expedition],[N_Condition],[LI_Principal]\n" +
                ",[LI_Telephone],[LI_Telecopie],[LI_EMail],[cbProt],[cbCreateur]\n" +
                ",[cbModification],[cbReplication],[cbFlag],cbMarqSource,DataBaseSource,LI_NoSource) \n" +
                "                                 \n" +
                "SELECT (SELECT MAX(LI_No) FROM F_LIVRAISON) + ROW_NUMBER() OVER(ORDER BY dest.LI_No),dest.[CT_Num],[LI_Intitule],[LI_Adresse],[LI_Complement],[LI_CodePostal],[LI_Ville]\n" +
                ",[LI_CodeRegion],[LI_Pays],[LI_Contact],[N_Expedition],[N_Condition],[LI_Principal]\n" +
                ",[LI_Telephone],[LI_Telecopie],[LI_EMail],[cbProt],[cbCreateur]\n" +
                ",[cbModification],[cbReplication],[cbFlag],dest.[cbMarqSource],dest.[dataBaseSource],dest.LI_No\n" +
                "FROM F_LIVRAISON_DEST dest \n" +
                "LEFT JOIN (SELECT [LI_NoSource],[CT_Num],[DatabaseSource] FROM F_LIVRAISON) src \n" +
                "ON dest.[LI_No] = src.[LI_NoSource] \n" +
                "AND dest.[CT_Num] = src.[CT_Num] \n" +
                "AND dest.[DatabaseSource] = src.[DatabaseSource] \n" +
                "WHERE src.[LI_NoSource] IS NULL ;" +
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
                "   'F_LIVRAISON',\n" +
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
                executeQuery(sqlCon, updateTableDest("", "'LI_NoSource','LI_No','CT_Num','DataBaseSource'", tableName, tableName + "_DEST", filename,unibase));
                sendData(sqlCon, path, filename, insert(filename));

                deleteTempTable(sqlCon, tableName + "_DEST");
                deleteLivraison(sqlCon, path, filename);
            }
        }
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"LI_No,CT_Num");
        getData(sqlCon, selectSourceTable(tableName,database,true), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }
    public static void deleteLivraison(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_LIVRAISON \n" +
                " WHERE EXISTS (SELECT 1 FROM F_LIVRAISON_SUPPR WHERE F_LIVRAISON_SUPPR.LI_No = F_LIVRAISON.LI_No AND F_LIVRAISON_SUPPR.CT_Num = F_LIVRAISON.CT_Num) \n" +
                " \n" +
                " IF OBJECT_ID('F_LIVRAISON_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_LIVRAISON_SUPPR \n";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }

}
