import java.io.File;
import java.sql.Connection;

public class ArtStock extends Table {

    public static String file = "ArtStock.csv";
    public static String tableName = "F_ARTSTOCK";
    public static String configList = "listArtStock";

    public static String insert()
    {
        return          "BEGIN TRY " +
                        "\n" +
                        "INSERT INTO [dbo].[F_ARTSTOCK]  \n" +
                        "([AR_Ref],[DE_No],[AS_QteMini],[AS_QteMaxi],[AS_MontSto],[AS_QteSto],[AS_QteRes],[AS_QteCom],[AS_Principal],[AS_QteResCM],[AS_QteComCM]\n" +
                        "      ,[AS_QtePrepa],[DP_NoPrincipal],[DP_NoControle],[AS_QteAControler],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag])  \n" +
                        "                  \n" +
                        "SELECT dest.[AR_Ref],dest.[DE_No],[AS_QteMini],[AS_QteMaxi],[AS_MontSto],[AS_QteSto],[AS_QteRes],[AS_QteCom],[AS_Principal],[AS_QteResCM],[AS_QteComCM]\n" +
                        "      ,[AS_QtePrepa],[DP_NoPrincipal],[DP_NoControle],[AS_QteAControler],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag] \n" +
                        "FROM F_ARTSTOCK_DEST dest    \n" +
                        "LEFT JOIN (SELECT [AR_Ref],[DE_No] FROM F_ARTSTOCK) src    \n" +
                        "ON dest.[AR_Ref] = src.[AR_Ref]    \n" +
                        "AND dest.DE_No = src.DE_No  \n" +
                        "WHERE src.[AR_Ref] IS NULL ;  \n" +
                        "IF OBJECT_ID('F_ARTSTOCK_DEST') IS NOT NULL  \n" +
                        "DROP TABLE F_ARTSTOCK_DEST;\n" +
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
                        "   'F_ARTSTOCK',\n" +
                        "   GETDATE());\n" +
                        "END CATCH";
    }

    public static void sendDataElement(Connection  sqlCon, String path,String database)
    {

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref,DE_No","'AR_Ref','DE_No'",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,insert());

        deleteTempTable(sqlCon,tableName);
        deleteArtStock(sqlCon, path);
    }

    public static void getDataElement(Connection  sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"AR_Ref,DE_No");
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList,database);
    }

    public static void deleteArtStock(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_ARTSTOCK \n" +
                " WHERE EXISTS (SELECT 1 FROM F_ARTSTOCK_SUPPR WHERE F_ARTSTOCK_SUPPR.AR_Ref = F_ARTSTOCK.AR_Ref" +
                "   AND F_ARTSTOCK_SUPPR.DE_No = F_ARTSTOCK.DE_No)  \n" +
                "  \n" +
                " IF OBJECT_ID('F_ARTSTOCK_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_ARTSTOCK_SUPPR ;";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }
}
