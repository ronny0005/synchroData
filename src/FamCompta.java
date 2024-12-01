import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class FamCompta extends Table {

    public static String file ="famCompta_";
    public static String tableName = "F_FAMCOMPTA";
    public static String configList = "listFamCompta";

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
                executeQuery(sqlCon, updateTableDest("FA_CodeFamille,FCP_Type,FCP_Champ", "FA_CodeFamille,FCP_Type,FCP_Champ", tableName, tableName + "_DEST", filename,unibase,0,""));
            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"","FA_CodeFamille,FCP_Type,FCP_Champ");
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"FA_CodeFamille,FCP_Type,FCP_Champ");
        getData(sqlCon, selectSourceTable(tableName,database,true,""), tableName, path, filename);
        File avroFile = new File(path + "//" + filename);
        if (avroFile.exists())
            executeQuery(sqlCon,updateSelectTable(tableName,true));
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

}
