import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class Comptet extends Table {

    public static String file = "comptet_";
    public static String tableName = "F_COMPTET";
    public static String configList = "listComptet";


    public static String updateDepotInsert() {

        return "UPDATE tmp SET DE_No = ISNULL(dep.DE_No,dest.[DE_No])\n" +
                "FROM F_COMPTET_TMP tmp\n" +
                "INNER JOIN F_COMPTET_DEST dest ON dest.[cbMarqSource] = dest.[cbMarqSource]\n" +
                "LEFT JOIN F_DEPOT dep ON ISNULL(dep.DE_NoSource,0) = ISNULL(dest.DE_No,0) AND ISNULL(dep.dataBaseSource,'') = ISNULL(dest.dataBaseSource,'')   \n" +
                "\n";
    }

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
                executeQuery(sqlCon, updateTableDest("CT_Num,CT_Type", "CT_Num,CT_Type,DE_No", tableName, tableName + "_DEST", filename,unibase,0,""));
                executeQuery(sqlCon,insertTmpTable (tableName,tableName+"_DEST","CT_Num,CT_Type",filename,0,0,"","","DE_No"));
                executeQuery(sqlCon,updateDepotInsert());
                executeQuery(sqlCon,insertTable (tableName,tableName+"_TMP","CT_Num,CT_Type",filename,0,0,"","",""));
                enableTrigger(sqlCon,tableName);

            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"","CT_Num");
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"CT_Num,CT_Type");
        getData(sqlCon, selectSourceTable(tableName,database,true,""), tableName, path, filename);
        File avroFile = new File(path + "//" + filename);
        if (avroFile.exists())
            executeQuery(sqlCon,updateSelectTable(tableName,true));
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

}
