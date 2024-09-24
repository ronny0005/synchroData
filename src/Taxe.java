import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class Taxe extends Table {
    public static String file ="taxe_";
    public static String tableName = "F_TAXE";
    public static String configList = "listTaxe";

    public static String insert(String filename)
    {
        return
                " BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                " \n" +

                " IF OBJECT_ID('F_TAXE_DEST') IS NOT NULL\n"+
                " INSERT INTO [dbo].[F_TAXE]\n" +
                "           ([TA_Intitule],[TA_TTaux],[TA_Taux],[TA_Type],[CG_Num],[TA_No],[TA_Code],[TA_NP]\n" +
                "           ,[TA_Sens],[TA_Provenance],[TA_Regroup],[TA_Assujet],[TA_GrilleBase],[TA_GrilleTaxe]\n" +
                "           ,[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag])\n" +
                "\n" +
                "\t\t   SELECT [TA_Intitule],[TA_TTaux],[TA_Taux],[TA_Type],[CG_Num],[TA_No],dest.[TA_Code],[TA_NP]\n" +
                "           ,[TA_Sens],[TA_Provenance],[TA_Regroup],[TA_Assujet],[TA_GrilleBase],[TA_GrilleTaxe]\n" +
                "           ,[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]\n" +
                "\t\t   FROM F_TAXE_DEST dest\n" +
                "\t\t   LEFT JOIN (SELECT TA_Code FROM F_TAXE) src\n" +
                "\t\t   ON\tdest.TA_Code = src.TA_Code\n" +
                "\t\t   WHERE src.TA_Code IS NULL;\n" +
                "\n" +
                "\t\t   IF OBJECT_ID('F_TAXE_DEST') IS NOT NULL\n" +
                "\t\t   DROP TABLE F_TAXE_DEST;" +
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
                "   'F_TAXE',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }

    public static void sendDataElement(Connection sqlCon, String path,int unibase) {
        File dir = new File(path);
        FilenameFilter filter = (dir1, name) -> name.startsWith(file);
        String[] children = dir.list(filter);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                readOnFile(path, filename, tableName + "_DEST", sqlCon);

                readOnFile(path, "deleteList" + filename, tableName + "_SUPPR", sqlCon);
                executeQuery(sqlCon, updateTableDest("TA_Code", "'TA_Code'", tableName, tableName + "_DEST", filename,unibase));
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","TA_Code",filename,0,0,"","",""));
//                sendData(sqlCon, path, filename, insert(filename));

                deleteTempTable(sqlCon, tableName + "_DEST");
                deleteArtCompta(sqlCon, path, filename);
            }
        }
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"TA_Code");
        getData(sqlCon, selectSourceTable(tableName,database,true), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

    public static void deleteArtCompta(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE src\n" +
                " FROM F_TAXE src\n" +
                " INNER JOIN F_TAXE_SUPPR del\n" +
                " ON src.TA_Code = del.TA_Code;\n" +
                " IF OBJECT_ID('F_TAXE_SUPPR') IS NOT NULL\n" +
                " DROP TABLE F_TAXE_SUPPR ;";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }

}
