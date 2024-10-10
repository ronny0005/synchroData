import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class Taxe extends Table {
    public static String file ="taxe_";
    public static String tableName = "F_TAXE";
    public static String configList = "listTaxe";

    public static void sendDataElement(Connection sqlCon, String path,int unibase) {
        File dir = new File(path);
        FilenameFilter filter = (dir1, name) -> name.startsWith(file);
        String[] children = dir.list(filter);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                executeQuery(sqlCon, updateTableDest("TA_Code", "'TA_Code'", tableName, tableName + "_DEST", filename,unibase));
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","TA_Code",filename,0,0,"","",""));

                deleteTempTable(sqlCon, tableName + "_DEST");

            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"","TA_Code");
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".avro";
        initTableParam(sqlCon,tableName,configList,"TA_Code");
        getData(sqlCon, selectSourceTable(tableName,database,true,""), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

}
