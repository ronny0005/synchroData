import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class Condition extends Table{

    public static String file = "condition_";
    public static String tableName = "F_CONDITION";
    public static String configList = "listCondition";

    public static String insert(String filename)
    {
        return  " BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                " IF OBJECT_ID('F_CONDITION_DEST') IS NOT NULL\n"+
                "\tINSERT INTO F_CONDITION (\n" +
                "\t[AR_Ref],[CO_No],[EC_Enumere],[EC_Quantite]\n" +
                "\t\t\t,[CO_Ref],[CO_CodeBarre],[CO_Principal],[cbProt],[cbCreateur]\n" +
                "\t\t\t,[cbModification],[cbReplication],[cbFlag])\n" +
                "                                \n" +
                "\tSELECT dest.[AR_Ref],dest.[CO_No],dest.[EC_Enumere],[EC_Quantite]\n" +
                "\t\t\t,[CO_Ref],[CO_CodeBarre],[CO_Principal],[cbProt],[cbCreateur]\n" +
                "\t\t\t,[cbModification],[cbReplication],[cbFlag]\n" +
                "\tFROM F_CONDITION_DEST dest\n" +
                "\tLEFT JOIN (SELECT AR_Ref,CO_No,EC_Enumere FROM F_CONDITION) src\n" +
                "\t\tON dest.CO_No = src.CO_No\n" +
                "\t\tAND dest.EC_Enumere = src.EC_Enumere\n" +
                "\t\tAND dest.AR_Ref = src.AR_Ref\n" +
                "\tWHERE src.CO_No IS NULL\n" +
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
                "   'F_CONDITION',\n" +
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
                executeQuery(sqlCon, updateTableDest("CO_No,AR_Ref", "'CO_No','AR_Ref'", tableName, tableName + "_DEST",filename));
                sendData(sqlCon, path, filename, insert(filename));

                deleteCondition(sqlCon, path,filename);
            }
        }
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"CO_No,AR_Ref");
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }
    public static void deleteCondition(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_CONDITION \n" +
                " WHERE CO_No IN(SELECT CO_No FROM F_CONDITION_SUPPR) \n" +
                " \n" +
                " IF OBJECT_ID('F_CONDITION_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_CONDITION_SUPPR \n";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }
}
