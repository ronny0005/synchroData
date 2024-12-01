import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class Livraison extends Table {

    public static String file ="livraison_";
    public static String tableName = "F_LIVRAISON";
    public static String configList = "listLivraison";

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
                executeQuery(sqlCon, updateTableDest("LI_No,CT_Num", "LI_NoSource,LI_No,CT_Num,DataBaseSource", tableName, tableName + "_DEST", filename,unibase,0,"LI_No"));

                executeQuery(sqlCon,insertTmpTable (tableName,tableName+"_DEST","LI_No,CT_Num,DatabaseSource",filename,0,1,"","LI_No",""));
                executeQuery(sqlCon,insertTable (tableName,tableName+"_TMP","LI_No,CT_Num,DatabaseSource",filename,1,0,"LI_No","",""));
            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"LI_No","CT_Num,DataBaseSource");
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"LI_No,CT_Num,DataBaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,true,"LI_No"), tableName, path, filename);
        File avroFile = new File(path + "//" + filename);
        if (avroFile.exists())
            executeQuery(sqlCon,updateSelectTable(tableName,true));
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

}
