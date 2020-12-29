import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class Compteg extends Table {

    public static String file = "compteg_";
    public static String tableName = "F_COMPTEG";
    public static String configList = "listCompteg";

    public static String insert()
    {
        return  "BEGIN TRY " +
                " \n" +
                "SET DATEFORMAT ymd;\n" +
                "INSERT INTO [dbo].[F_COMPTEG]\n" +
                "           ([CG_Num],[CG_Type],[CG_Intitule],[CG_Classement]\n" +
                "           ,[N_Nature],[CG_Report],[CR_Num],[CG_Raccourci]\n" +
                "           ,[CG_Saut],[CG_Regroup],[CG_Analytique],[CG_Echeance]\n" +
                "           ,[CG_Quantite],[CG_Lettrage],[CG_Tiers],[CG_DateCreate]\n" +
                "           ,[CG_Devise],[N_Devise],[TA_Code],[CG_Sommeil]\n" +
                "           ,[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag])\n" +
                "     SELECT dest.[CG_Num],[CG_Type],[CG_Intitule],[CG_Classement]\n" +
                "           ,[N_Nature],[CG_Report],[CR_Num],[CG_Raccourci]\n" +
                "           ,[CG_Saut],[CG_Regroup],[CG_Analytique],[CG_Echeance]\n" +
                "           ,[CG_Quantite],[CG_Lettrage],[CG_Tiers],[CG_DateCreate]\n" +
                "           ,[CG_Devise],[N_Devise],[TA_Code],[CG_Sommeil]\n" +
                "           ,[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]\n" +
                "FROM\t[F_COMPTEG_DEST] dest\n" +
                "LEFT JOIN (SELECT CG_Num FROM F_COMPTEG) src\n" +
                "\tON\tdest.CG_Num = src.CG_Num\n" +
                "WHERE\tsrc.CG_Num IS NULL" +
                "            \n" +
                "IF OBJECT_ID('F_COMPTEG_DEST') IS NOT NULL \n" +
                "DROP TABLE F_COMPTEG_DEST \n" +
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
                "   'insert',\n"+
                "   'F_COMPTEG',\n" +
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
                executeQuery(sqlCon, updateTableDest("CG_Num,CG_Type", "'CG_Num','CG_Type'", tableName, tableName + "_DEST"));
                sendData(sqlCon, path, filename, insert());

                deleteTempTable(sqlCon, tableName);
                deleteCompteg(sqlCon, path,filename);
            }
        }
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"CG_Num");
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

    public static void deleteCompteg(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_COMPTEG \n" +
                " WHERE CG_Num IN(SELECT CG_Num FROM F_COMPTEG_SUPPR) \n" +
                " \n" +
                " IF OBJECT_ID('F_COMPTEG_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_COMPTEG_SUPPR \n";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }

}
