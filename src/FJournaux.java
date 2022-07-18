import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class FJournaux extends Table {

    public static String file = "fjournaux_";
    public static String tableName = "F_JOURNAUX";
    public static String configList = "listFJournaux";

    public static String insert(String filename)
    {
        return  "BEGIN TRY " +
                " \n" +
                "SET DATEFORMAT ymd;\n" +

                " IF OBJECT_ID('F_JOURNAUX_DEST') IS NOT NULL\n"+
                "INSERT INTO F_JOURNAUX (\n" +
                "[JO_Num],[JO_Intitule],[CG_Num],[JO_Type]\n" +
                "      ,[JO_NumPiece],[JO_Contrepartie],[JO_SaisAnal],[JO_NotCalcTot]\n" +
                "      ,[JO_Rappro],[JO_Sommeil],[JO_IFRS],[JO_Reglement]\n" +
                "      ,[JO_SuiviTreso],[cbProt],[cbCreateur],[cbModification]\n" +
                "      ,[cbReplication],[cbFlag])\n" +
                "            \n" +
                "SELECT dest.[JO_Num],[JO_Intitule],[CG_Num],[JO_Type]\n" +
                "      ,[JO_NumPiece],[JO_Contrepartie],[JO_SaisAnal],[JO_NotCalcTot]\n" +
                "      ,[JO_Rappro],[JO_Sommeil],[JO_IFRS],[JO_Reglement]\n" +
                "      ,[JO_SuiviTreso],[cbProt],[cbCreateur],[cbModification]\n" +
                "      ,[cbReplication],[cbFlag]\n" +
                "FROM F_JOURNAUX_DEST dest\n" +
                "LEFT JOIN (SELECT JO_Num FROM F_JOURNAUX) src\n" +
                "\tON\tdest.JO_Num = src.JO_Num\n" +
                "WHERE src.JO_Num IS NULL\n" +
                "            \n" +
                "IF OBJECT_ID('F_JOURNAUX_DEST') IS NOT NULL \n" +
                "DROP TABLE F_JOURNAUX_DEST \n" +
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
                "   'F_COMPTET',\n" +
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
                executeQuery(sqlCon, updateTableDest("JO_Num,JO_Type", "'JO_Num','JO_Type','CG_Num','JO_IFRS'", tableName, tableName + "_DEST", filename));
                sendData(sqlCon, path, filename, insert(filename));

                deleteTempTable(sqlCon, tableName + "_DEST");
                deleteJournaux(sqlCon, path, filename);
            }
        }
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"JO_Num,JO_Type");
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

    public static void deleteJournaux(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_JOURNAUX \n" +
                " WHERE JO_Num IN(SELECT JO_Num FROM F_JOURNAUX_SUPPR) \n" +
                " \n" +
                " IF OBJECT_ID('F_JOURNAUX_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_JOURNAUX_SUPPR \n";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }

}
