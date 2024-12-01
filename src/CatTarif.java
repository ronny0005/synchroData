import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class CatTarif extends Table {

    public static String file ="catTarif_";
    public static String tableName = "P_CATTARIF";
    public static String configList = "listCatTarif";
    public static String keyColumns = "cbIndice";

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
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                executeQuery(sqlCon, updateTableDest(keyColumns, "cbIndice", tableName, tableName + "_DEST", filename,unibase,0,""));
                executeQuery(sqlCon, updateTableDest("cbIndice", "", tableName, tableName + "_DEST", filename,0,0,""));
            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"","cbIndice");
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,keyColumns);//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,database,true,""), tableName, path, filename);
        File avroFile = new File(path + "//" + filename);
        if (avroFile.exists())
            executeQuery(sqlCon,updateSelectTable(tableName,true));
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }
    public static void initTable(Connection sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='P_CATTARIF') \n" +
                "     INSERT INTO config.ListCatTarif\n" +
                "     SELECT cbIndice,cbMarq \n" +
                "     FROM P_CATTARIF";
        executeQuery(sqlCon, query);
    }
}
