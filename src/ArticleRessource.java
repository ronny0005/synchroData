import java.io.File;
import java.sql.Connection;

public class ArticleRessource extends Table {

    public static String file ="articleressource.csv";
    public static String tableName = "F_ARTICLERESSOURCE";
    public static String configList = "listArtRessource";
    public static String list()
    {
        return "SELECT\t[AR_Ref],[RP_Code],[cbProt],[cbMarq]\n" +
                "\t\t,[cbCreateur],[cbModification],[cbReplication],[cbFlag]\n" +
                " ,cbMarqSource = cbMarq " +
                " FROM\t[F_ARTICLERESSOURCE] " +
                "WHERE cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='F_ARTICLERESSOURCE'),'1900-01-01')";
    }

    public static String insert()
    {
        return "BEGIN TRY" +
                " SET DATEFORMAT ymd;\n" +
                "\tUPDATE F_ARTICLERESSOURCE \n" +
                "\tSET  [cbProt] = F_ARTICLERESSOURCE_DEST.cbProt\n" +
                "      ,[cbCreateur] = F_ARTICLERESSOURCE_DEST.cbCreateur\n" +
                "      ,[cbModification] = F_ARTICLERESSOURCE_DEST.cbModification\n" +
                "      ,[cbReplication] = F_ARTICLERESSOURCE_DEST.cbReplication\n" +
                "      ,[cbFlag] = F_ARTICLERESSOURCE_DEST.cbFlag\n" +
                "\tFROM F_ARTICLERESSOURCE_DEST\n" +
                "\tWHERE F_ARTICLERESSOURCE.AR_Ref = F_ARTICLERESSOURCE_DEST.AR_Ref\n" +
                "\t\tAND F_ARTICLERESSOURCE.RP_Code = F_ARTICLERESSOURCE_DEST.RP_Code\n" +
                "                                \n" +
                "\tINSERT INTO F_ARTICLERESSOURCE (\n" +
                "\t[AR_Ref],[RP_Code],[cbProt]\n" +
                "\t\t\t,[cbCreateur],[cbModification],[cbReplication],[cbFlag])\n" +
                "                                \n" +
                "\tSELECT dest.[AR_Ref],dest.[RP_Code],[cbProt]\n" +
                "\t\t\t,[cbCreateur],[cbModification],[cbReplication],[cbFlag]\n" +
                "\tFROM F_ARTICLERESSOURCE_DEST dest\n" +
                "\tLEFT JOIN (SELECT AR_Ref,RP_Code FROM F_ARTICLERESSOURCE) src\n" +
                "\t\tON dest.AR_Ref = src.AR_Ref\n" +
                "\t\tAND dest.RP_Code = src.RP_Code\n" +
                "\tWHERE src.AR_Ref IS NULL;\n" +
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
                "   'F_ARTICLERESSOURCE',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }

    public static void sendDataElement(Connection sqlCon, String path,String database)
    {

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        /*readOnFile(path,file,"F_ARTICLERESSOURCE_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"F_ARTICLERESSOURCE_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());

         */
        deleteArticleRessource(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        /*initTable(sqlCon);
        getData(sqlCon, list(), "F_ARTICLERESSOURCE", path, file);
        listDeleteArticleRessource(sqlCon, path);*/
    }
    public static void initTable(Connection sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_ARTICLERESSOURCE') " +
                " INSERT INTO config.ListArticleRessource " +
                " SELECT RP_Code,AR_Ref,cbMarq " +
                " FROM F_ARTICLERESSOURCE ";
        executeQuery(sqlCon, query);
    }
    public static void deleteArticleRessource(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_ARTICLERESSOURCE \n" +
                " WHERE EXISTS (SELECT 1 FROM F_ARTICLERESSOURCE_SUPPR WHERE F_ARTICLERESSOURCE_SUPPR.AR_Ref = F_ARTICLERESSOURCE.AR_Ref AND F_ARTICLERESSOURCE_SUPPR.RP_Code = F_ARTICLERESSOURCE.RP_Code) \n" +
                " \n" +
                " IF OBJECT_ID('F_ARTICLERESSOURCE_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_ARTICLERESSOURCE_SUPPR \n";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }

    public static void listDeleteArticleRessource(Connection sqlCon, String path)
    {
        String query = " SELECT lart.RP_Code,lart.AR_Ref,lart.cbMarq " +
                " FROM config.ListArticleRessource lart " +
                " LEFT JOIN dbo.F_ARTICLERESSOURCE fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\deleteList" + file, query, sqlCon);

        query = " DELETE FROM config.ListArticleRessource " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_ARTICLERESSOURCE " +
                "                  WHERE dbo.F_ARTICLERESSOURCE.cbMarq = config.ListArticleRessource.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
