import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class ArtCompta extends Table {
    public static String file ="ArtCompta_";
    public static String tableName = "F_ARTCOMPTA";
    public static String configList = "listArtCompta";

    public static String insert(String filename)
    {
        return
                " BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
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
                "   'Insert '+ ' "+filename+"',\n" +
                "   'F_ARTCOMPTA',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }

    public static void sendDataElement(Connection sqlCon, String path,String database) {
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
                executeQuery(sqlCon, updateTableDest("AR_Ref,ACP_Type,ACP_Champ", "'AR_Ref','ACP_Type','ACP_Champ'", tableName, tableName + "_DEST",filename));
                sendData(sqlCon, path, filename, insert(filename));

                deleteTempTable(sqlCon, tableName);
                deleteArtCompta(sqlCon, path,filename);
            }
        }
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"AR_Ref,ACP_Type,ACP_Champ");
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

    public static void deleteArtCompta(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_ARTCOMPTA  \n" +
                " WHERE EXISTS (SELECT 1 FROM F_ARTCOMPTA_SUPPR WHERE F_ARTCOMPTA_SUPPR.AR_Ref = F_ARTCOMPTA.AR_Ref" +
                "   AND F_ARTCOMPTA_SUPPR.ACP_Type = F_ARTCOMPTA.ACP_Type AND F_ARTCOMPTA_SUPPR.ACP_Champ = F_ARTCOMPTA.ACP_Champ)  \n" +
                "  \n" +
                " IF OBJECT_ID('F_ARTCOMPTA_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_ARTCOMPTA_SUPPR ;";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }

}
