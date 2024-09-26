import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class Condition extends Table{

    public static String file = "condition_";
    public static String tableName = "F_CONDITION";
    public static String configList = "listCondition";

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
                executeQuery(sqlCon, updateTableDest("CO_No,AR_Ref,EC_Enumere", "'CO_No','AR_Ref'", tableName, tableName + "_DEST", filename,unibase));
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","CO_No,AR_Ref,EC_Enumere",filename,0,0,"","",""));

            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"","CO_No,AR_Ref,EC_Enumere");
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"CO_No,AR_Ref");
        getData(sqlCon, selectSourceTable(tableName,database,true), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }
}
