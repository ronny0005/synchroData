import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class FamCompta extends Table {

    public static String file ="famCompta_";
    public static String tableName = "F_FAMCOMPTA";
    public static String configList = "listFamCompta";

    public static String insert(String filename)
    {
        return          " BEGIN TRY " +

                        " SET DATEFORMAT ymd;\n" +
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
                        "   'Insert '+ ' "+filename+"',\n" +
                        "   'F_FAMCOMPTA',\n" +
                        "   GETDATE());\n" +
                        "END CATCH";

    }

    public static void sendDataElement(Connection sqlCon, String path,String database)
    {
        File dir = new File(path);
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith(file);
            }
        };
        String[] children = dir.list(filter);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (int i = 0; i < children.length; i++) {
                String filename = children[i];
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                readOnFile(path, "deleteList" + filename, tableName + "_SUPPR", sqlCon);
                executeQuery(sqlCon, updateTableDest("FA_CodeFamille,FCP_Type,FCP_Champ", "'FA_CodeFamille','FCP_Type','FCP_Champ'", tableName, tableName + "_DEST",filename));
                sendData(sqlCon, path, filename, insert(filename));
                deleteTempTable(sqlCon, tableName);

                deleteFamCompta(sqlCon, path,filename);
            }
        }
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"FA_CodeFamille,FCP_Type,FCP_Champ");
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }
    public static void deleteFamCompta(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_FAMCOMPTA  \n" +
                " WHERE EXISTS (SELECT 1 FROM F_FAMCOMPTA_SUPPR WHERE F_FAMCOMPTA_SUPPR.FA_CodeFamille = F_FAMCOMPTA.FA_CodeFamille" +
                "   AND F_FAMCOMPTA_SUPPR.FCP_Type = F_FAMCOMPTA.FCP_Type AND F_FAMCOMPTA_SUPPR.FCP_Champ = F_FAMCOMPTA.FCP_Champ)  \n" +
                "  \n" +
                " IF OBJECT_ID('F_FAMCOMPTA_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_FAMCOMPTA_SUPPR ;";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }

}
