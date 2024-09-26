import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class Comptet extends Table {

    public static String file = "comptet_";
    public static String tableName = "F_COMPTET";
    public static String configList = "listComptet";

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

                disableTrigger(sqlCon,tableName);
                executeQuery(sqlCon, updateTableDest("CT_Num,CT_Type", "'CT_Num','CT_Type','DE_No'", tableName, tableName + "_DEST", filename,unibase));

                deleteTempTable(sqlCon, tableName + "_DEST");
                enableTrigger(sqlCon,tableName);

            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"","CT_Num");
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"CT_Num,CT_Type");
        getData(sqlCon, selectSourceTable(tableName,database,true), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

}
