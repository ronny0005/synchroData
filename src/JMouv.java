import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class JMouv extends Table {
    public static String file ="JMouv_";
    public static String tableName = "F_JMOUV";
    public static String configList = "listJMouv";

    public static void sendDataElement(Connection sqlCon, String path,int unibase) {
        deleteAllTable(sqlCon,tableName);
        File dir = new File(path);
        FilenameFilter filter = (dir1, name) -> name.startsWith(file);
        String[] children = dir.list(filter);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {

                importFiles(sqlCon, tableName,path,filename);
                disableTrigger(sqlCon,tableName);
                executeQuery(sqlCon, updateTableDest("JO_Num,JM_Date", "JO_Num,JM_Date", tableName, tableName + "_DEST", filename,unibase,0,""));
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","JO_Num,JM_Date",filename,0,0,"","",""));
                enableTrigger(sqlCon,tableName);
            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"","JO_Num,JM_Date");
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"JO_Num,JM_Date");
        getData(sqlCon, selectSourceTable(tableName,database,true,""), tableName, path, filename);
        File avroFile = new File(path + "//" + filename);
        if (avroFile.exists())
            executeQuery(sqlCon,updateSelectTable(tableName,true));
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

}
