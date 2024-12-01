import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class Condition extends Table{

    public static String file = "condition_";
    public static String tableName = "F_CONDITION";
    public static String configList = "listCondition";

    public static void sendDataElement(Connection sqlCon, String path,int unibase)
    {
        deleteAllTable(sqlCon,tableName);
        File dir = new File(path);
        FilenameFilter filter = (dir1, name) -> name.startsWith(file);
        String[] children = dir.list(filter);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                importFiles(sqlCon, tableName,path,filename);
                executeQuery(sqlCon,insertTmpTable (tableName,tableName+"_DEST","AR_Ref,EC_Enumere",filename,0,1,"","CO_No","CO_Ref,CO_CodeBarre"));
                executeQuery(sqlCon,insertTable (tableName,tableName+"_TMP","AR_Ref,EC_Enumere",filename,1,1,"CO_No","CO_No","CO_Ref,CO_CodeBarre"));
                executeQuery(sqlCon, updateTableDest("AR_Ref,EC_Enumere", "CO_No,AR_Ref", tableName, tableName + "_DEST", filename,unibase,1,"CO_No"));

            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"","CO_No,AR_Ref,EC_Enumere");
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"CO_No,AR_Ref,EC_Enumere");
        getData(sqlCon, selectSourceTable(tableName,database,true,""), tableName, path, filename);
        File avroFile = new File(path + "//" + filename);
        if (avroFile.exists())
            executeQuery(sqlCon,updateSelectTable(tableName,true));
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }
}
