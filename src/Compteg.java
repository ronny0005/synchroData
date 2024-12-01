import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class Compteg extends Table {

    public static String file = "compteg_";
    public static String tableName = "F_COMPTEG";
    public static String configList = "listCompteg";
    public static String keyColumns = "CG_Num,CG_Type";

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
                executeQuery(sqlCon, updateTableDest(keyColumns, "CG_Num,CG_Type", tableName, tableName + "_DEST", filename,unibase,0,""));
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","CG_Num",filename,0,0,"","",""));

                deleteTempTable(sqlCon, tableName + "_DEST");

            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"","CG_Num");
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"CG_Num");
        getData(sqlCon, selectSourceTable(tableName,database,true,""), tableName, path, filename);
        File avroFile = new File(path + "//" + filename);
        if (avroFile.exists())
            executeQuery(sqlCon,updateSelectTable(tableName,true));
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

}
