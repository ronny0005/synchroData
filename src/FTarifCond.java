import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class FTarifCond extends Table{

    public static String file = "ftarifcond_";
    public static String tableName = "F_TARIFCOND";
    public static String configList = "listFTarifCond";

    public static String insert(String filename)
    {
        return  " BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                " IF OBJECT_ID('F_TARIFCOND_DEST') IS NOT NULL\n"+
                "\tINSERT INTO F_TARIFCOND (\n" +
                "\t[AR_Ref],[TC_RefCF],[CO_No],[TC_Prix],[TC_PrixNouv],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag])\n" +
                "                                \n" +
                "\tSELECT dest.[AR_Ref],dest.[TC_RefCF],dest.[CO_No],dest.[TC_Prix],dest.[TC_PrixNouv],dest.[cbProt],dest.[cbCreateur],dest.[cbModification],dest.[cbReplication],dest.[cbFlag]\n" +
                "\tFROM F_TARIFCOND_DEST dest\n" +
                "\tLEFT JOIN (SELECT AR_Ref,CO_No FROM F_TARIFCOND) src\n" +
                "\t\tON dest.AR_Ref = src.AR_Ref\n" +
                "\t\tAND dest.CO_No = src.CO_No\n" +
                "\tWHERE src.AR_Ref IS NULL\n" +
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
                "   'F_TARIFCOND',\n" +
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
                executeQuery(sqlCon, updateTableDest("AR_Ref,CO_No", "'AR_Ref','CO_No','TC_RefCF'", tableName, tableName + "_DEST", filename,unibase));
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","AR_Ref,CO_No",filename,0,0,"","",""));
                //sendData(sqlCon, path, filename, insert(filename));

                deleteCondition(sqlCon, path, filename);
            }
        }
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"AR_Ref,CO_No");
        getData(sqlCon, selectSourceTable(tableName,database,true), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }
    public static void deleteCondition(Connection sqlCon, String path,String filename)
    {
        String query =

                " DELETE src\n" +
                        " FROM F_TARIFCOND src\n" +
                        " INNER JOIN F_TARIFCOND_SUPPR del\n" +
                        " ON src.AR_Ref = del.AR_Ref \n" +
                        " AND src.CO_No = del.CO_No ;\n" +
                        " IF OBJECT_ID('F_TARIFCOND_SUPPR') IS NOT NULL\n" +
                        " DROP TABLE F_TARIFCOND_SUPPR ;";

        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }
}
