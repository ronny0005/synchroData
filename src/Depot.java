import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class Depot extends Table{

    public static String tableName = "F_DEPOT";
    public static String configList = "listDepot";
    public static String file ="depot_";

    public static void linkDepot(Connection sqlCon)
    {
        String query = "\n" +
                "UPDATE dep SET DP_NoDefaut = depE.DP_No\n" +
                "FROM F_DEPOT dep\n" +
                "INNER JOIN F_DEPOT_DEST depD\n" +
                "ON dep.DE_NoSource = depD.DE_No\n" +
                "AND dep.DataBaseSource = depD.DataBaseSource\n" +
                "INNER JOIN F_DEPOTEMPL depE\n" +
                "ON depE.DP_NoSource = depD.DP_NoDefaut\n" +
                "AND depE.DataBaseSource = depD.DataBaseSource";
        executeQuery(sqlCon, query);
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

                disableTrigger(sqlCon,tableName);
                importFiles(sqlCon, tableName,path,filename);
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","DE_No,DatabaseSource",filename,1,1,"DE_No","DE_No","DP_NoDefaut,DE_Code"));
                executeQuery(sqlCon, updateTableDest("DE_No,DatabaseSource", "DE_No,DP_NoDefaut,DE_Code,DE_NoSource,DatabaseSource,DE_Intitule", tableName, tableName + "_DEST", filename,unibase,1,"DE_No"));
                enableTrigger(sqlCon,tableName);
                DepotEmpl.sendDataElement(sqlCon, path,unibase);
                linkDepot(sqlCon);
            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"DE_No","DataBaseSource");

    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"DE_No,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,true,"DE_No"), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

        DepotEmpl.getDataElement(sqlCon, path,database, time);
    }
    public static void getDataElementFilterAgency(Connection sqlCon, String path,String database,String time,String agency)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"DE_No,DatabaseSource");
        getData(sqlCon, selectSourceTableFilterAgency(tableName,database,agency,"DE_No,DatabaseSource"), tableName, path, filename);
        File avroFile = new File(path + "//" + filename);
        if (avroFile.exists())
            executeQuery(sqlCon,updateSelectTable(tableName,true));
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

        DepotEmpl.getDataElement(sqlCon, path,database, time);
    }
}
