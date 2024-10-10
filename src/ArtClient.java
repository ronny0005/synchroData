import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class ArtClient extends Table{
    public static String file ="ArtClient_";
    public static String tableName = "F_ARTCLIENT";
    public static String configList = "listArtClient";
    public static String keyColumns = "AR_Ref,AC_Categorie";

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
                executeQuery(sqlCon, updateTableDest(keyColumns, "'AR_Ref','AC_Categorie','CT_Num'", tableName, tableName + "_DEST", filename,unibase));
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST",keyColumns,filename,0,0,"","",""));
                deleteTempTable(sqlCon, tableName + "_DEST");
            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"",keyColumns);
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".avro";
        initTableParam(sqlCon,tableName,configList,keyColumns);//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,database,true,""), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }
}
