import java.io.File;
import java.sql.Connection;

public class FamCompta extends Table {

    public static String file ="famCompta.csv";
    public static String tableName = "F_FAMCOMPTA";
    public static String configList = "listFamCompta";

    public static String list()
    {
        return  "SELECT [FA_CodeFamille],[FCP_Type],[FCP_Champ],[FCP_ComptaCPT_CompteG]\n" +
                "      ,[FCP_ComptaCPT_CompteA],[FCP_ComptaCPT_Taxe1],[FCP_ComptaCPT_Taxe2],[FCP_ComptaCPT_Taxe3]\n" +
                "      ,[FCP_TypeFacture],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]\n" +
                "\t  ,cbMarqSource = [cbMarq]\n" +
                "FROM\t[F_FAMCOMPTA]\n" +
                "WHERE cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='F_FAMCOMPTA'),'1900-01-01')";
    }

    public static String insert()
    {
        return          " BEGIN TRY " +

                        " SET DATEFORMAT ymd;\n" +
                        " UPDATE F_FAMCOMPTA    \n" +
                        " SET  [FCP_Type] = [F_FAMCOMPTA_DEST].FCP_Type\n" +
                        "      ,[FCP_Champ] = [F_FAMCOMPTA_DEST].FCP_Champ\n" +
                        "      ,[FCP_ComptaCPT_CompteG] = [F_FAMCOMPTA_DEST].FCP_ComptaCPT_CompteG\n" +
                        "      ,[FCP_ComptaCPT_CompteA] = [F_FAMCOMPTA_DEST].FCP_ComptaCPT_CompteA\n" +
                        "      ,[FCP_ComptaCPT_Taxe1] = [F_FAMCOMPTA_DEST].FCP_ComptaCPT_Taxe1\n" +
                        "      ,[FCP_ComptaCPT_Taxe2] = [F_FAMCOMPTA_DEST].FCP_ComptaCPT_Taxe2\n" +
                        "      ,[FCP_ComptaCPT_Taxe3] = [F_FAMCOMPTA_DEST].FCP_ComptaCPT_Taxe3\n" +
                        "      ,[FCP_TypeFacture] = [F_FAMCOMPTA_DEST].FCP_TypeFacture\n" +
                        "      ,[cbProt] = [F_FAMCOMPTA_DEST].cbProt\n" +
                        "      ,[cbCreateur] = [F_FAMCOMPTA_DEST].cbCreateur\n" +
                        "      ,[cbModification] = [F_FAMCOMPTA_DEST].cbModification\n" +
                        "      ,[cbReplication] = [F_FAMCOMPTA_DEST].cbReplication\n" +
                        "      ,[cbFlag] = [F_FAMCOMPTA_DEST].cbFlag\n" +
                        " FROM \tF_FAMCOMPTA_DEST   \n" +
                        " WHERE \tF_FAMCOMPTA.FA_CodeFamille = F_FAMCOMPTA_DEST.FA_CodeFamille \n" +
                        " AND \tF_FAMCOMPTA.FCP_Type = F_FAMCOMPTA_DEST.FCP_Type \n" +
                        " AND \tF_FAMCOMPTA.FCP_Champ = F_FAMCOMPTA_DEST.FCP_Champ \n" +
                        " \n" +
                        " INSERT INTO [dbo].[F_FAMCOMPTA] \n" +
                        "\t ([FA_CodeFamille],[FCP_Type],[FCP_Champ],[FCP_ComptaCPT_CompteG]\n" +
                        "      ,[FCP_ComptaCPT_CompteA],[FCP_ComptaCPT_Taxe1],[FCP_ComptaCPT_Taxe2],[FCP_ComptaCPT_Taxe3]\n" +
                        "      ,[FCP_TypeFacture],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]) \n" +
                        "\t\n" +
                        " SELECT dest.[FA_CodeFamille],dest.[FCP_Type],dest.[FCP_Champ],[FCP_ComptaCPT_CompteG]\n" +
                        "      ,[FCP_ComptaCPT_CompteA],[FCP_ComptaCPT_Taxe1],[FCP_ComptaCPT_Taxe2],[FCP_ComptaCPT_Taxe3]\n" +
                        "      ,[FCP_TypeFacture],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag] \n" +
                        " FROM F_FAMCOMPTA_DEST dest   \n" +
                        " LEFT JOIN (SELECT [FA_CodeFamille],[FCP_Type],[FCP_Champ] FROM F_FAMCOMPTA) src   \n" +
                        " ON dest.[FA_CodeFamille] = src.[FA_CodeFamille]   \n" +
                        " AND \tdest.FCP_Type = src.FCP_Type \n" +
                        " AND \tdest.FCP_Champ = src.FCP_Champ \n" +
                        " WHERE src.[FA_CodeFamille] IS NULL ; \n" +
                        " IF OBJECT_ID('F_FAMCOMPTA_DEST') IS NOT NULL \n" +
                        " DROP TABLE F_FAMCOMPTA_DEST;" +
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
                        "   'F_FAMCOMPTA',\n" +
                        "   GETDATE());\n" +
                        "END CATCH";

    }

    public static void deleteTempTable(Connection sqlCon)
    {
        String query = "IF OBJECT_ID('F_FAMCOMPTA_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_FAMCOMPTA_DEST;";
        executeQuery(sqlCon, query);
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        /*readOnFile(path,file,"F_FAMCOMPTA_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"F_FAMCOMPTA_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());

         */
        deleteTempTable(sqlCon);
        deleteFamCompta(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        /*initTable(sqlCon);
        getData(sqlCon, list(), "F_FAMCOMPTA", path, file);
        listDeleteFamCompta(sqlCon, path);

         */
    }
    public static void initTable(Connection sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_FAMCOMPTA') \n" +
                "     INSERT INTO config.ListFamCompta\n" +
                "     SELECT FA_CodeFamille,FCP_Type,FCP_Champ,cbMarq \n" +
                "     FROM F_FAMCOMPTA";
        executeQuery(sqlCon, query);
    }
    public static void deleteFamCompta(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_FAMCOMPTA  \n" +
                " WHERE EXISTS (SELECT 1 FROM F_FAMCOMPTA_SUPPR WHERE F_FAMCOMPTA_SUPPR.FA_CodeFamille = F_FAMCOMPTA.FA_CodeFamille" +
                "   AND F_FAMCOMPTA_SUPPR.FCP_Type = F_FAMCOMPTA.FCP_Type AND F_FAMCOMPTA_SUPPR.FCP_Champ = F_FAMCOMPTA.FCP_Champ)  \n" +
                "  \n" +
                " IF OBJECT_ID('F_FAMCOMPTA_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_FAMCOMPTA_SUPPR ;";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }

    public static void listDeleteFamCompta(Connection sqlCon, String path)
    {
        String query = " SELECT lart.FA_CodeFamille,lart.FCP_Type,lart.FCP_Champ,lart.cbMarq " +
                " FROM config.ListFamCompta lart " +
                " LEFT JOIN dbo.F_FAMCOMPTA fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\deleteList" + file, query, sqlCon);

        query = " DELETE FROM config.ListFamCompta " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_FAMCOMPTA " +
                "                  WHERE dbo.F_FAMCOMPTA.cbMarq = config.ListFamCompta.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
