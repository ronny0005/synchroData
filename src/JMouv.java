import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class JMouv extends Table {
    public static String file ="JMouv_";
    public static String tableName = "F_JMOUV";
    public static String configList = "listJMouv";

    public static String insert(String filename)
    {
        return
                " BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                " \n" +

                " IF OBJECT_ID('F_JMOUV_DEST') IS NOT NULL\n"+
                " INSERT INTO [dbo].[F_JMOUV] \n" +
                "\t ([JO_Num],[JM_Date],[JM_Cloture],[JM_Impression],[JM_DateCloture]\n" +
                "\t ,[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]) \n" +
                "\t\n" +
                "SELECT dest.[JO_Num],dest.[JM_Date],[JM_Cloture],[JM_Impression],[JM_DateCloture] \n" +
                "\t    ,[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag] \n" +
                " FROM F_JMOUV_DEST dest   \n" +
                " LEFT JOIN (SELECT [JO_Num],[JM_Date] FROM F_JMOUV) src   \n" +
                " ON dest.[JO_Num] = src.[JO_Num]   \n" +
                " AND \tdest.JM_Date = src.JM_Date \n" +
                " WHERE src.[JO_Num] IS NULL ; \n" +
                " IF OBJECT_ID('F_JMOUV_DEST') IS NOT NULL \n" +
                " DROP TABLE F_JMOUV_DEST;\n" +
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
                "   'F_JMOUV',\n" +
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
                executeQuery(sqlCon, updateTableDest("JO_Num,JM_Date", "'JO_Num','JM_Date'", tableName, tableName + "_DEST", filename,unibase));
                sendData(sqlCon, path, filename, insert(filename));

                deleteTempTable(sqlCon, tableName + "_DEST");
                deleteArtCompta(sqlCon, path, filename);
            }
        }
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"JO_Num,JM_Date");
        getData(sqlCon, selectSourceTable(tableName,database,true), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

    public static void deleteArtCompta(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_JMOUV  \n" +
                " WHERE EXISTS (SELECT 1 FROM F_JMOUV_SUPPR WHERE F_JMOUV_SUPPR.JO_Num = F_JMOUV.JO_Num" +
                "   AND F_JMOUV_SUPPR.JM_Date = F_JMOUV.JM_Date)  \n" +
                "  \n" +
                " IF OBJECT_ID('F_JMOUV_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_JMOUV_SUPPR ;";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }

}
