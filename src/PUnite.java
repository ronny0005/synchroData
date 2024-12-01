import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class PUnite extends Table{

    public static String file = "punite_";
    public static String tableName = "P_UNITE";
    public static String configList = "listPUnite";

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
                executeQuery(sqlCon, updateTableDest("cbIndice", "cbIndice", tableName, tableName + "_DEST", filename,unibase,0,""));
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","cbIndice",filename,0,0,"","",""));
            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"","cbIndice");
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"cbIndice");
        getData(sqlCon, selectSourceTable(tableName,database,false,""), tableName, path, filename);
        File avroFile = new File(path + "//" + filename);
        if (avroFile.exists())
            executeQuery(sqlCon,updateSelectTable(tableName,true));
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }
}
