import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class EcritureC extends Table {

    public static String file = "EcritureC_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_ECRITUREC";
    public static String configList = "listEcritureC";

    public static String updateDateValue(String nullDateValue){
            return "DECLARE @updateCol VARCHAR(MAX) \n" +
                    "DECLARE @nullValueDate DATE = '"+ nullDateValue +"'\n" +
                    "\n" +
                    "SELECT\t@updateCol = STRING_AGG(CONCAT('UPDATE F_ECRITUREC_DEST SET ',col.name,' = ''',@nullValueDate,''' WHERE YEAR(',col.name,') = 1900'),';')\n" +
                    "FROM sys.tables tab\n" +
                    "INNER JOIN sys.columns col\n" +
                    "ON tab.object_id = col.object_id\n" +
                    "INNER JOIN sys.types t\n" +
                    "ON col.user_type_id = t.user_type_id\n" +
                    "WHERE tab.name = 'F_ECRITUREC'\n" +
                    "AND t.name LIKE '%date%'\n" +
                    "AND col.name NOT LIKE 'cb%'\n" +
                    "\n" +
                    "exec sp_executesql @updateCol";
    }

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
                executeQuery(sqlCon,updateDateValue("1753-01-01"));
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
