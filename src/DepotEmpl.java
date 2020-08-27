import java.io.File;
import java.sql.Connection;

public class DepotEmpl extends Table {

    public static String file = "depotEmpl.csv";
    public static String tableName = "F_DEPOTEMPL";
    public static String configList = "listDepotEmpl";

    public static String insert()
    {
        return          "BEGIN TRY " +
                        " SET DATEFORMAT ymd;\n" +
                        " UPDATE [dbo].[F_DEPOTEMPL]  \n" +
                        "   SET [DP_Code] = F_DEPOTEMPL_DEST.DP_Code\n" +
                        "      ,[DP_Intitule] = F_DEPOTEMPL_DEST.DP_Intitule\n" +
                        "      ,[DP_Zone] = F_DEPOTEMPL_DEST.DP_Zone\n" +
                        "      ,[DP_Type] = F_DEPOTEMPL_DEST.DP_Type\n" +
                        "      ,[cbProt] = F_DEPOTEMPL_DEST.cbProt\n" +
                        "      ,[cbCreateur] = F_DEPOTEMPL_DEST.cbCreateur\n" +
                        "      ,[cbModification] = F_DEPOTEMPL_DEST.cbModification\n" +
                        "      ,[cbReplication] = F_DEPOTEMPL_DEST.cbReplication\n" +
                        "      ,[cbFlag] = F_DEPOTEMPL_DEST.cbFlag\n" +
                        "FROM F_DEPOTEMPL_DEST       \n" +
                        "  WHERE F_DEPOTEMPL.DE_No = F_DEPOTEMPL_DEST.DE_No \n" +
                        "  \n" +
                        "  INSERT INTO [dbo].[F_DEPOTEMPL]     \n" +
                        "  ([DE_No],[DP_No],[DP_Code],[DP_Intitule],[DP_Zone],[DP_Type],[cbProt]\n" +
                        "      ,[cbCreateur],[cbModification],[cbReplication],[cbFlag])     \n" +
                        "     \n" +
                        "  SELECT dest.[DP_No],[DE_No],[DP_Code],[DP_Intitule],[DP_Zone],[DP_Type],[cbProt]\n" +
                        "      ,[cbCreateur],[cbModification],[cbReplication],[cbFlag] \n" +
                        "  FROM F_DEPOTEMPL_DEST dest       \n" +
                        "  LEFT JOIN (SELECT DP_No FROM F_DEPOTEMPL) src       \n" +
                        "  ON dest.DP_No = src.DP_No\n" +
                        "  WHERE src.DP_No IS NULL ;     \n" +
                        "  IF OBJECT_ID('F_DEPOTEMPL_DEST') IS NOT NULL     \n" +
                        "  DROP TABLE F_DEPOTEMPL_DEST;" +
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
                        "   'F_DEPOTEMPL',\n" +
                        "   GETDATE());\n" +
                        "END CATCH";
    }

    public static void sendDataElement(Connection sqlCon, String path,String database)
    {

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "DP_No","'DP_No','DE_No'",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,insert());

        deleteTempTable(sqlCon,tableName);
        deleteDepot(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"DP_No");
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList,database);
    }
    public static void deleteDepot(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_DEPOTEMPL  \n" +
                " WHERE EXISTS (SELECT 1 FROM F_DEPOTEMPL_SUPPR WHERE F_DEPOTEMPL_SUPPR.DP_No = F_DEPOTEMPL.DP_No" +
                "   )  \n" +
                "  \n" +
                " IF OBJECT_ID('F_DEPOTEMPL_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_DEPOTEMPL_SUPPR ;";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }
}
