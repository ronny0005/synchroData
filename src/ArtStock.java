import java.io.File;
import java.sql.Connection;

public class ArtStock extends Table {

    public static String file = "ArtStock.csv";
    public static String tableName = "F_ARTSTOCK";
    public static String configList = "listArtStock";
    public static String list()
    {
        return   "SELECT\t[AR_Ref],[DE_No],[AS_QteMini],[AS_QteMaxi],[AS_MontSto],[AS_QteSto],[AS_QteRes],[AS_QteCom],[AS_Principal]\n" +
                "\t\t,[AS_QteResCM],[AS_QteComCM],[AS_QtePrepa],[DP_NoPrincipal],[DP_NoControle],[AS_QteAControler],[cbProt]\n" +
                "\t\t,[cbCreateur],[cbModification],[cbReplication],[cbFlag],cbMarqSource = [cbMarq]\n" +
                "FROM\t[F_ARTSTOCK]\n" +
                "WHERE cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='F_ARTSTOCK'),'1900-01-01')";
    }

    public static String insert()
    {
        return          "BEGIN TRY " +
                        "\n" +
                        "UPDATE F_ARTSTOCK     \n" +
                        "SET  [AS_QteMini] = F_ARTSTOCK_DEST.AS_QteMini\n" +
                        "      ,[AS_QteMaxi] = F_ARTSTOCK_DEST.AS_QteMaxi\n" +
                        "      ,[AS_MontSto] = F_ARTSTOCK_DEST.AS_MontSto\n" +
                        "      ,[AS_QteSto] = F_ARTSTOCK_DEST.AS_QteSto\n" +
                        "      ,[AS_QteRes] = F_ARTSTOCK_DEST.AS_QteRes\n" +
                        "      ,[AS_QteCom] = F_ARTSTOCK_DEST.AS_QteCom\n" +
                        "      ,[AS_Principal] = F_ARTSTOCK_DEST.AS_Principal\n" +
                        "      ,[AS_QteResCM] = F_ARTSTOCK_DEST.AS_QteResCM\n" +
                        "      ,[AS_QteComCM] = F_ARTSTOCK_DEST.AS_QteComCM\n" +
                        "      ,[AS_QtePrepa] = F_ARTSTOCK_DEST.AS_QtePrepa\n" +
                        "      ,[DP_NoPrincipal] = F_ARTSTOCK_DEST.DP_NoPrincipal\n" +
                        "      ,[DP_NoControle] = F_ARTSTOCK_DEST.DP_NoControle\n" +
                        "      ,[AS_QteAControler] = F_ARTSTOCK_DEST.AS_QteAControler\n" +
                        "      ,[cbProt] = F_ARTSTOCK_DEST.cbProt\n" +
                        "      ,[cbCreateur] = F_ARTSTOCK_DEST.cbCreateur\n" +
                        "      ,[cbModification] = F_ARTSTOCK_DEST.cbModification\n" +
                        "      ,[cbReplication] = F_ARTSTOCK_DEST.cbReplication\n" +
                        "      ,[cbFlag] = F_ARTSTOCK_DEST.cbFlag \n" +
                        "FROM F_ARTSTOCK_DEST    \n" +
                        "WHERE F_ARTSTOCK.AR_Ref = F_ARTSTOCK_DEST.AR_Ref  \n" +
                        "AND F_ARTSTOCK.DE_No = F_ARTSTOCK_DEST.DE_No\n" +
                        "                   \n" +
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
                        "   'F_ARTSTOCK',\n" +
                        "   GETDATE());\n" +
                        "END CATCH";
    }

    public static void deleteTempTable(Connection  sqlCon)
    {
        String query = "IF OBJECT_ID('F_ARTSTOCK_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_ARTSTOCK_DEST;";
        executeQuery(sqlCon, query);
    }
    public static void sendDataElement(Connection  sqlCon, String path,String database)
    {

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        /*readOnFile(path,file,"F_ARTSTOCK_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"F_ARTSTOCK_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());

         */
        deleteTempTable(sqlCon);
        deleteArtStock(sqlCon, path);
    }
    public static void getDataElement(Connection  sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        /*initTable(sqlCon);
        getData(sqlCon, list(), "F_ARTSTOCK", path, file);
        listDeleteArtStock(sqlCon, path);

         */
    }
    public static void initTable(Connection  sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_ARTSTOCK') \n" +
                "     INSERT INTO config.ListArtStock\n" +
                "     SELECT AR_Ref,DE_No,cbMarq \n" +
                "     FROM F_ARTSTOCK";
        executeQuery(sqlCon, query);
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

    public static void listDeleteArtStock(Connection sqlCon, String path)
    {
        String query = " SELECT lart.AR_Ref,lart.DE_No,lart.cbMarq " +
                " FROM config.ListArtStock lart " +
                " LEFT JOIN dbo.F_ARTSTOCK fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\deleteList" + file, query, sqlCon);

        query = " DELETE FROM config.ListArtStock " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_ARTSTOCK " +
                "                  WHERE dbo.F_ARTSTOCK.cbMarq = config.ListArtStock.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
