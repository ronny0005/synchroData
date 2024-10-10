import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class FEnumCond extends Table{

    public static String file = "fenumcond_";
    public static String tableName = "F_ENUMCOND";
    public static String configList = "listFEnumCond";

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
                executeQuery(sqlCon, updateTableDest("EC_Enumere,EC_Champ", "'EC_Enumere','EC_Champ'", tableName, tableName + "_DEST", filename,unibase));
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","EC_Champ,EC_Enumere",filename,0,0,"","",""));


            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"","EC_Champ,EC_Enumere");
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".avro";
        initTableParam(sqlCon,tableName,configList,"EC_Enumere,EC_Champ");
        getData(sqlCon, selectSourceTable(tableName,database,true,""), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }
}
