import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class ArtFourniss extends Table {

    public static String file ="ArtFourniss_";
    public static String tableName = "F_ARTFOURNISS";
    public static String configList = "listArtFourniss";
    public static String keyColumns = "AR_Ref,CT_Num";

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
                disableTrigger(sqlCon,tableName);
                executeQuery(sqlCon, updateTableDest(keyColumns, keyColumns, tableName, tableName + "_DEST", filename,unibase,0,""));
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST",keyColumns,filename,0,0,"","",""));
                enableTrigger(sqlCon,tableName);
            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"","AR_Ref,CT_Num");
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,keyColumns);//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,database,true,"")/*list()*/, tableName, path, filename);
        File avroFile = new File(path + "//" + filename);
        if (avroFile.exists())
            executeQuery(sqlCon,updateSelectTable(tableName,true));
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

}
