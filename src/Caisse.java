import java.io.File;
import java.sql.Connection;

public class Caisse extends Table {
    public static String file ="caisse_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_CAISSE";
    public static String configList = "listCaisse";
    public static String keyColumns = "CA_No";

    public static void sendDataElement(Connection sqlCon, String path,String database,int unibase)
    {
        dbSource = database;
        loadFile(path,sqlCon);
        loadDeleteFile(path,sqlCon,file,tableName,"CA_No","DatabaseSource");
        //DocEntete.loadDeleteFile(path,sqlCon,file,tableName,deleteQuery());;
    }

    public static String updateDepotInsert(){

        return "UPDATE tmp SET DE_No = ISNULL(dep.DE_No,dest.[DE_No])\n" +
                "FROM F_CAISSE_TMP tmp\n" +
                "INNER JOIN F_CAISSE_DEST dest ON tmp.cbMarqSource = dest.cbMarqSource\n" +
                "LEFT JOIN (SELECT DE_NoSource,DatabaseSource,DE_No FROM F_DEPOT) dep \n" +
                "ON ISNULL(dep.DE_NoSource,0) = ISNULL(dest.DE_No,0) \n" +
                "AND ISNULL(dep.DataBaseSource,'') = ISNULL(dest.DataBaseSource,'') " +
                "\n";
    }


    public static void loadFile(String path,Connection sqlCon){
        deleteAllTable(sqlCon,tableName);
        String [] children = getFile(path,file);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children){
                importFiles(sqlCon, tableName,path,filename);
                executeQuery(sqlCon,insertTmpTable (tableName,tableName+"_DEST","CA_No,DatabaseSource",filename,0,1,"","CA_No","DE_No"));
                executeQuery(sqlCon,updateDepotInsert());
                executeQuery(sqlCon,insertTable (tableName,tableName+"_TMP","CA_No,DatabaseSource",filename,1,0,"CA_No","",""));
                executeQuery(sqlCon, updateTableDest("CA_No,DatabaseSource", "", tableName, tableName + "_TMP", filename,0,1,"CA_No"));
            }
        }
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"CA_No,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,true,keyColumns), tableName, path, filename);
        File avroFile = new File(path + "//" + filename);
        if (avroFile.exists())
            executeQuery(sqlCon,updateSelectTable(tableName,true));
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

}
