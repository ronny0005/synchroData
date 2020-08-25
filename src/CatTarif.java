import java.io.File;
import java.sql.Connection;

public class CatTarif extends Table {

    public static String file ="catTarif.csv";
    public static String tableName = "P_CATTARIF";
    public static String configList = "listCatTarif";
    public static String list()
    {
        return "SELECT\t[CT_Intitule],[CT_PrixTTC],[cbIndice]\n" +
                "\t  ,cbMarqSource = [cbMarq]\n" +
                "FROM\t[P_CATTARIF]";
    }

    public static String insert()
    {
        return          " BEGIN TRY " +
                        " SET DATEFORMAT ymd;\n" +
                        " UPDATE P_CATTARIF    \n" +
                        " SET  [CT_Intitule] = [P_CATTARIF_DEST].[CT_Intitule]\n" +
                        "      ,[CT_PrixTTC] = [P_CATTARIF_DEST].[CT_PrixTTC]\n" +
                        " FROM P_CATTARIF_DEST   \n" +
                        " WHERE P_CATTARIF.[cbIndice] = P_CATTARIF_DEST.[cbIndice] \n" +
                        " \n" +
                        " INSERT INTO [dbo].[P_CATTARIF] \n" +
                        " ([cbIndice],[CT_Intitule],[CT_PrixTTC]) \n" +
                        "\n" +
                        " SELECT dest.[cbIndice],[CT_PrixTTC],[CT_Intitule]\n" +
                        " FROM P_CATTARIF_DEST dest   \n" +
                        " LEFT JOIN (SELECT [cbIndice] FROM P_CATTARIF) src   \n" +
                        " ON dest.[cbIndice] = src.[cbIndice] \n" +
                        " WHERE src.[cbIndice] IS NULL ; \n" +
                        " IF OBJECT_ID('P_CATTARIF_DEST') IS NOT NULL \n" +
                        " DROP TABLE P_CATTARIF_DEST;" +
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
                        "   'P_CATTARIF',\n" +
                        "   GETDATE());\n" +
                        "END CATCH";
    }

    public static void deleteTempTable(Connection sqlCon)
    {
        String query = "IF OBJECT_ID('P_CATTARIF_DEST') IS NOT NULL \n" +
                "\tDROP TABLE P_CATTARIF_DEST;";
        executeQuery(sqlCon, query);
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        /*readOnFile(path,file,"P_CATTARIF_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"P_CATTARIF_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());

         */
        deleteTempTable(sqlCon);
        deleteCatTarif(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        /*initTable(sqlCon);
        getData(sqlCon, list(), "P_CATTARIF", path, file);
        listDeleteCatTarif(sqlCon, path);*/
    }
    public static void initTable(Connection sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='P_CATTARIF') \n" +
                "     INSERT INTO config.ListCatTarif\n" +
                "     SELECT cbIndice,cbMarq \n" +
                "     FROM P_CATTARIF";
        executeQuery(sqlCon, query);
    }
    public static void deleteCatTarif(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM P_CATTARIF  \n" +
                " WHERE EXISTS (SELECT 1 FROM P_CATTARIF_SUPPR WHERE P_CATTARIF_SUPPR.cbIndice = P_CATTARIF.cbIndice)  \n" +
                "  \n" +
                " IF OBJECT_ID('P_CATTARIF_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE P_CATTARIF_SUPPR ;";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }

    public static void listDeleteCatTarif(Connection sqlCon, String path)
    {
        String query = " SELECT lart.cbIndice,lart.cbMarq " +
                " FROM config.ListCatTarif lart " +
                " LEFT JOIN dbo.P_CATTARIF fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\deleteList" + file, query, sqlCon);

        query = " DELETE FROM config.ListCatTarif " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM P_CATTARIF " +
                "                  WHERE dbo.P_CATTARIF.cbMarq = config.ListFamCompta.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
