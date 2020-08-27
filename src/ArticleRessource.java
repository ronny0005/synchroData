import java.io.File;
import java.sql.Connection;

public class ArticleRessource extends Table {

    public static String file ="articleressource.csv";
    public static String tableName = "F_ARTICLERESSOURCE";
    public static String configList = "ListArticleRessource";
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
                "   'insert',\n" +
                "   'F_ARTICLERESSOURCE',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }

    public static void sendDataElement(Connection sqlCon, String path,String database)
    {
        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "RP_Code,AR_Ref","'RP_Code'",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,insert());

        deleteArticleRessource(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"RP_Code,AR_Ref");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList,database);
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

}
