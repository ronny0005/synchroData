import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class CatTarif extends Table {

    public static String file ="catTarif_";
    public static String tableName = "P_CATTARIF";
    public static String configList = "listCatTarif";

    public static String insert(String filename)
    {
        return          " BEGIN TRY " +
                        " SET DATEFORMAT ymd;\n" +
                        " \n" +
                        " IF OBJECT_ID('P_CATTARIF_DEST') IS NOT NULL\n"+
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
                        "   'Insert '+ ' "+filename+"',\n" +
                        "   'P_CATTARIF',\n" +
                        "   GETDATE());\n" +
                        "END CATCH";
    }

    public static void sendDataElement(Connection sqlCon, String path)
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
                executeQuery(sqlCon, updateTableDest("cbIndice", "'cbIndice'", tableName, tableName + "_DEST", filename));
                sendData(sqlCon, path, filename, insert(filename));

                deleteTempTable(sqlCon, tableName + "_DEST");
                deleteCatTarif(sqlCon, path, filename);
            }
        }
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"cbIndice");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }
    public static void initTable(Connection sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='P_CATTARIF') \n" +
                "     INSERT INTO config.ListCatTarif\n" +
                "     SELECT cbIndice,cbMarq \n" +
                "     FROM P_CATTARIF";
        executeQuery(sqlCon, query);
    }
    public static void deleteCatTarif(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM P_CATTARIF  \n" +
                " WHERE EXISTS (SELECT 1 FROM P_CATTARIF_SUPPR WHERE P_CATTARIF_SUPPR.cbIndice = P_CATTARIF.cbIndice)  \n" +
                "  \n" +
                " IF OBJECT_ID('P_CATTARIF_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE P_CATTARIF_SUPPR ;";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }
}
