import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class EcritureC extends Table {

    public static String file = "EcritureC_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_ECRITUREC";
    public static String configList = "listEcritureC";

    public static void sendDataElement(Connection  sqlCon, String path,String database,int unibase)
    {
        deleteAllTable(sqlCon,tableName);
        File dir = new File(path);
        FilenameFilter filter = (dir1, name) -> name.startsWith(file);
        String[] children = dir.list(filter);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                dbSource = database;
                importFiles(sqlCon, tableName,path,filename);

                disableTrigger(sqlCon,tableName);
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","cbMarqSource,dataBaseSource",filename,1,1,"EC_No","EC_No",""));
                executeQuery(sqlCon, updateTableDest("EC_No", "EC_No,JM_Date,JO_Num,EC_CType", tableName, tableName + "_DEST", filename,unibase,0,"EC_No"));
                enableTrigger(sqlCon,tableName);
            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"EC_No","dataBaseSource");
    }
    public static void getDataElement(Connection  sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"EC_No,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,true,"EC_No"), tableName, path, filename);
        File avroFile = new File(path + "//" + filename);
        if (avroFile.exists())
            executeQuery(sqlCon,updateSelectTable(tableName,true));
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }

}
