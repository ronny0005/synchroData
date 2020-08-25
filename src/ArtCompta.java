import java.io.File;
import java.sql.Connection;

public class ArtCompta extends Table {
    public static String file ="ArtCompta.csv";
    public static String tableName = "F_ARTCOMPTA";
    public static String configList = "listArtCompta";

    public static String list()
    {
        return "SELECT [AR_Ref],[ACP_Type],[ACP_Champ],[ACP_ComptaCPT_CompteG]\n" +
                "      ,[ACP_ComptaCPT_CompteA],[ACP_ComptaCPT_Taxe1],[ACP_ComptaCPT_Taxe2],[ACP_ComptaCPT_Taxe3]\n" +
                "      ,[ACP_TypeFacture],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]\n" +
                "\t  ,cbMarqSource = [cbMarq]\n" +
                "FROM\t[F_ARTCOMPTA]\n" +
                "WHERE cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='F_ARTCOMPTA'),'1900-01-01')";
    }

    public static String insert()
    {
        return
                        " BEGIN TRY " +
                        " SET DATEFORMAT ymd;\n" +
                        " UPDATE F_ARTCOMPTA    \n" +
                        " SET  [ACP_Type] = [F_ARTCOMPTA_DEST].ACP_Type\n" +
                        "      ,[ACP_Champ] = [F_ARTCOMPTA_DEST].ACP_Champ\n" +
                        "      ,[ACP_ComptaCPT_CompteG] = [F_ARTCOMPTA_DEST].ACP_ComptaCPT_CompteG\n" +
                        "      ,[ACP_ComptaCPT_CompteA] = [F_ARTCOMPTA_DEST].ACP_ComptaCPT_CompteA\n" +
                        "      ,[ACP_ComptaCPT_Taxe1] = [F_ARTCOMPTA_DEST].ACP_ComptaCPT_Taxe1\n" +
                        "      ,[ACP_ComptaCPT_Taxe2] = [F_ARTCOMPTA_DEST].ACP_ComptaCPT_Taxe2\n" +
                        "      ,[ACP_ComptaCPT_Taxe3] = [F_ARTCOMPTA_DEST].ACP_ComptaCPT_Taxe3\n" +
                        "      ,[ACP_TypeFacture] = [F_ARTCOMPTA_DEST].ACP_TypeFacture\n" +
                        "      ,[cbProt] = [F_ARTCOMPTA_DEST].cbProt\n" +
                        "      ,[cbCreateur] = [F_ARTCOMPTA_DEST].cbCreateur\n" +
                        "      ,[cbModification] = [F_ARTCOMPTA_DEST].cbModification\n" +
                        "      ,[cbReplication] = [F_ARTCOMPTA_DEST].cbReplication\n" +
                        "      ,[cbFlag] = [F_ARTCOMPTA_DEST].cbFlag\n" +
                        " FROM \tF_ARTCOMPTA_DEST   \n" +
                        " WHERE \tF_ARTCOMPTA.AR_Ref = F_ARTCOMPTA_DEST.AR_Ref \n" +
                        " AND \tF_ARTCOMPTA.ACP_Type = F_ARTCOMPTA_DEST.ACP_Type \n" +
                        " AND \tF_ARTCOMPTA.ACP_Champ = F_ARTCOMPTA_DEST.ACP_Champ \n" +
                        " \n" +
                        " INSERT INTO [dbo].[F_ARTCOMPTA] \n" +
                        "\t ([AR_Ref],[ACP_Type],[ACP_Champ],[ACP_ComptaCPT_CompteG]\n" +
                        "      ,[ACP_ComptaCPT_CompteA],[ACP_ComptaCPT_Taxe1],[ACP_ComptaCPT_Taxe2],[ACP_ComptaCPT_Taxe3]\n" +
                        "      ,[ACP_TypeFacture],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]) \n" +
                        "\t\n" +
                        " SELECT dest.[AR_Ref],dest.[ACP_Type],dest.[ACP_Champ],[ACP_ComptaCPT_CompteG]\n" +
                        "      ,[ACP_ComptaCPT_CompteA],[ACP_ComptaCPT_Taxe1],[ACP_ComptaCPT_Taxe2],[ACP_ComptaCPT_Taxe3]\n" +
                        "      ,[ACP_TypeFacture],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag] \n" +
                        " FROM F_ARTCOMPTA_DEST dest   \n" +
                        " LEFT JOIN (SELECT [AR_Ref],[ACP_Type],[ACP_Champ] FROM F_ARTCOMPTA) src   \n" +
                        " ON dest.[AR_Ref] = src.[AR_Ref]   \n" +
                        " AND \tdest.ACP_Type = src.ACP_Type \n" +
                        " AND \tdest.ACP_Champ = src.ACP_Champ \n" +
                        " WHERE src.[AR_Ref] IS NULL ; \n" +
                        " IF OBJECT_ID('F_ARTCOMPTA_DEST') IS NOT NULL \n" +
                        " DROP TABLE F_ARTCOMPTA_DEST;\n" +
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
                        "   'F_ARTCOMPTA',\n" +
                        "   GETDATE());\n" +
                        "END CATCH";
    }

    public static void deleteTempTable(Connection sqlCon)
    {
        String query = "IF OBJECT_ID('F_ARTCOMPTA_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_ARTCOMPTA_DEST;";
        executeQuery(sqlCon, query);
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {
        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
/*      readOnFile(path,file,"F_ARTCOMPTA_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"F_ARTCOMPTA_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());
  */
        deleteTempTable(sqlCon);
        deleteArtCompta(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        /*
        initTable(sqlCon);
        getData(sqlCon, list(), "F_ARTCOMPTA", path, file);
        listDeleteArtCompta(sqlCon, path);

         */
    }
    public static void initTable(Connection sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_ARTCOMPTA') \n" +
                "     INSERT INTO config.ListArtCompta\n" +
                "     SELECT AR_Ref,ACP_Type,ACP_Champ,cbMarq \n" +
                "     FROM F_ARTCOMPTA";
        executeQuery(sqlCon, query);
    }
    public static void deleteArtCompta(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_ARTCOMPTA  \n" +
                " WHERE EXISTS (SELECT 1 FROM F_ARTCOMPTA_SUPPR WHERE F_ARTCOMPTA_SUPPR.AR_Ref = F_ARTCOMPTA.AR_Ref" +
                "   AND F_ARTCOMPTA_SUPPR.ACP_Type = F_ARTCOMPTA.ACP_Type AND F_ARTCOMPTA_SUPPR.ACP_Champ = F_ARTCOMPTA.ACP_Champ)  \n" +
                "  \n" +
                " IF OBJECT_ID('F_ARTCOMPTA_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_ARTCOMPTA_SUPPR ;";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }

    public static void listDeleteArtCompta(Connection sqlCon, String path)
    {
        String query = " SELECT lart.AR_Ref,lart.ACP_Type,lart.ACP_Champ,lart.cbMarq " +
                " FROM config.ListArtCompta lart " +
                " LEFT JOIN dbo.F_ARTCOMPTA fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\deleteList" + file, query, sqlCon);

        query = " DELETE FROM config.ListArtCompta " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_ARTCOMPTA " +
                "                  WHERE dbo.F_ARTCOMPTA.cbMarq = config.ListArtCompta.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
