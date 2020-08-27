import java.io.File;
import java.sql.Connection;

public class Livraison extends Table {

    public static String file ="livraison.csv";
    public static String tableName = "F_LIVRAISON";
    public static String configList = "listLivraison";

    public static String insert()
    {
        return  "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                "INSERT INTO F_LIVRAISON ( [LI_No],[CT_Num],[LI_Intitule],[LI_Adresse],[LI_Complement],[LI_CodePostal],[LI_Ville]\n" +
                ",[LI_CodeRegion],[LI_Pays],[LI_Contact],[N_Expedition],[N_Condition],[LI_Principal]\n" +
                ",[LI_Telephone],[LI_Telecopie],[LI_EMail],[cbProt],[cbCreateur]\n" +
                ",[cbModification],[cbReplication],[cbFlag]) \n" +
                "                                 \n" +
                "SELECT dest.[LI_No],dest.[CT_Num],[LI_Intitule],[LI_Adresse],[LI_Complement],[LI_CodePostal],[LI_Ville]\n" +
                ",[LI_CodeRegion],[LI_Pays],[LI_Contact],[N_Expedition],[N_Condition],[LI_Principal]\n" +
                ",[LI_Telephone],[LI_Telecopie],[LI_EMail],[cbProt],[cbCreateur]\n" +
                ",[cbModification],[cbReplication],[cbFlag]\n" +
                "FROM F_LIVRAISON_DEST dest \n" +
                "LEFT JOIN (SELECT [LI_No],[CT_Num] FROM F_LIVRAISON) src \n" +
                "ON dest.[LI_No] = src.[LI_No] \n" +
                "AND dest.[CT_Num] = src.[CT_Num] \n" +
                "WHERE src.[LI_No] IS NULL ;" +
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
                "   'F_LIVRAISON',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }

    public static void sendDataElement(Connection sqlCon, String path,String database)
    {

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "LI_No,CT_Num","'LI_No','CT_Num'",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,insert());

        deleteTempTable(sqlCon,tableName);
        deleteLivraison(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"LI_No,CT_Num");
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList,database);
    }
    public static void deleteLivraison(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_LIVRAISON \n" +
                " WHERE EXISTS (SELECT 1 FROM F_LIVRAISON_SUPPR WHERE F_LIVRAISON_SUPPR.LI_No = F_LIVRAISON.LI_No AND F_LIVRAISON_SUPPR.CT_Num = F_LIVRAISON.CT_Num) \n" +
                " \n" +
                " IF OBJECT_ID('F_LIVRAISON_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_LIVRAISON_SUPPR \n";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }

}
