import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class DepotEmpl extends Table {

    public static String file = "depotEmpl_";
    public static String tableName = "F_DEPOTEMPL";
    public static String configList = "listDepotEmpl";

    public static String updateDepotInsert(){
        return "UPDATE dest SET DE_No = ISNULL(srcDep.DE_No,dest.[DE_No])\n" +
                "FROM F_DEPOTEMPL_TMP dest\n" +
                "LEFT JOIN (SELECT DatabaseSource,DE_NoSource,DE_No FROM F_DEPOT) srcDep\n" +
                "ON ISNULL(dest.DE_No,0) = ISNULL(srcDep.DE_NoSource,0)\n" +
                "AND ISNULL(dest.DataBaseSource,'') = ISNULL(srcDep.DataBaseSource,'')\n" +
                "\n" +
                "DELETE \n" +
                "FROM F_DEPOTEMPL_TMP\n" +
                "WHERE DE_No IS NOT NULL";
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
                executeQuery(sqlCon, updateTableDest("", "'DP_No','DE_No','DP_NoSource'", tableName, tableName + "_DEST", filename,unibase));
                executeQuery(sqlCon,insertTmpTable (tableName,tableName+"_DEST","DP_No,DatabaseSource",filename,1,1,"DP_No","DP_No","DE_No"));
                executeQuery(sqlCon,updateDepotInsert());
                executeQuery(sqlCon,insertTable (tableName,tableName+"_TMP","DP_No",filename,0,0,"","",""));
            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"DP_No","DataBaseSource");

    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"DP_No,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,true), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }
}
