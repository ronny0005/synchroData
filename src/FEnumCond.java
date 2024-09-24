import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class FEnumCond extends Table{

    public static String file = "fenumcond_";
    public static String tableName = "F_ENUMCOND";
    public static String configList = "listFEnumCond";

    public static String insert(String filename)
    {
        return  " BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                " IF OBJECT_ID('F_ENUMCOND_DEST') IS NOT NULL\n"+
                "\tINSERT INTO F_ENUMCOND (\n" +
                "\t[EC_Champ],[EC_Enumere],[EC_Quantite],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag])\n" +
                "                                \n" +
                "\tSELECT dest.[EC_Champ],dest.[EC_Enumere],dest.[EC_Quantite],dest.[cbProt],dest.[cbCreateur],dest.[cbModification],dest.[cbReplication],dest.[cbFlag]\n" +
                "\tFROM F_ENUMCOND_DEST dest\n" +
                "\tLEFT JOIN (SELECT EC_Champ,EC_Enumere FROM F_ENUMCOND) src\n" +
                "\t\tON dest.EC_Champ = src.EC_Champ\n" +
                "\t\tAND dest.EC_Enumere = src.EC_Enumere\n" +
                "\tWHERE src.EC_Enumere IS NULL\n" +
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
                "   'F_ENUMCOND',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }
    public static void sendDataElement(Connection sqlCon, String path,int unibase)
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
                executeQuery(sqlCon, updateTableDest("EC_Enumere,EC_Champ", "'EC_Enumere','EC_Champ'", tableName, tableName + "_DEST", filename,unibase));
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","EC_Champ,EC_Enumere",filename,0,0,"","",""));
                //sendData(sqlCon, path, filename, insert(filename));

                deleteCondition(sqlCon, path, filename);
            }
        }
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"EC_Enumere,EC_Champ");
        getData(sqlCon, selectSourceTable(tableName,database,true), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }
    public static void deleteCondition(Connection sqlCon, String path,String filename)
    {
        String query =

                " DELETE src\n" +
                " FROM F_ENUMCOND src\n" +
                " INNER JOIN F_ENUMCOND_SUPPR del\n" +
                " ON src.EC_Enumere = del.EC_Enumere \n" +
                " AND src.EC_Champ = del.EC_Champ ;\n" +
                " IF OBJECT_ID('F_ENUMCOND_SUPPR') IS NOT NULL\n" +
                " DROP TABLE F_ENUMCOND_SUPPR ;";

        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }
}
