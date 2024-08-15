import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class PUnite extends Table{

    public static String file = "punite_";
    public static String tableName = "P_UNITE";
    public static String configList = "listPUnite";

    public static String insert(String filename)
    {
        return  " BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                " IF OBJECT_ID('P_UNITE_DEST') IS NOT NULL\n"+
                "\tINSERT INTO P_UNITE (\n" +
                "\t[U_Intitule],[U_Correspondance],[U_NbUnite],[U_UniteTemps],[cbIndice])\n" +
                "                                \n" +
                "\tSELECT dest.[U_Intitule],dest.[U_Correspondance],dest.[U_NbUnite],dest.[U_UniteTemps],dest.[cbIndice]\n" +
                "\tFROM P_UNITE_DEST dest\n" +
                "\tLEFT JOIN (SELECT cbIndice FROM P_UNITE) src\n" +
                "\t\tON dest.cbIndice = src.cbIndice\n" +
                "\tWHERE src.cbIndice IS NULL\n" +
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
                "   'P_UNITE',\n" +
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
                executeQuery(sqlCon, updateTableDest("cbIndice", "'cbIndice'", tableName, tableName + "_DEST", filename,unibase));
                sendData(sqlCon, path, filename, insert(filename));

                deleteCondition(sqlCon, path, filename);
            }
        }
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"cbIndice");
        getData(sqlCon, selectSourceTable(tableName,database,false), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }
    public static void deleteCondition(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM P_UNITE \n" +
                " WHERE cbIndice IN(SELECT cbIndice FROM P_UNITE_SUPPR) \n" +
                " \n" +
                " IF OBJECT_ID('P_UNITE_SUPPR') IS NOT NULL \n" +
                " DROP TABLE P_UNITE_SUPPR \n";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }
}
