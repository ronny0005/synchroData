import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class Livraison extends Table {

    public static String file ="livraison_";
    public static String tableName = "F_LIVRAISON";
    public static String configList = "listLivraison";

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
                executeQuery(sqlCon, updateTableDest("", "'LI_NoSource','LI_No','CT_Num','DataBaseSource'", tableName, tableName + "_DEST", filename,unibase));

            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"LI_No","CT_Num,DataBaseSource");
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"LI_No,CT_Num");
        getData(sqlCon, selectSourceTable(tableName,database,true), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

}
