import org.json.simple.JSONObject;

import java.io.File;
import java.sql.Connection;

public class DocLigne extends Table {

    public static String file = "DocLigne_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_DOCLIGNE";
    public static String configList = "listDocLigne";

    public static void sendDataElement(Connection sqlCon, String path,String database,int unibase)
    {
        dbSource = database;
        loadFile(path,sqlCon);
        loadDeleteFile(path,sqlCon);
    }

    public static String updateDepot(){
        return  "UPDATE tmp SET DE_No = ISNULL(dsrc.[DE_No],dest.DE_No) \n"+
                " FROM F_DOCLIGNE_TMP tmp  \n" +
                " INNER JOIN F_DOCLIGNE_DEST dest ON tmp.cbMarqSource = dest.cbMarqSource  \n" +
                " LEFT JOIN F_DEPOT dsrc  \n" +
                "  ON ISNULL(dsrc.DE_NoSource,0) = ISNULL(dest.DE_No,0)  \n" +
                "  AND ISNULL(dsrc.dataBaseSource,'') = ISNULL(dest.dataBaseSource,'')  \n";
    }

    public static void loadFile(String path,Connection sqlCon){
        deleteAllTable(sqlCon,tableName);
        String [] children = getFile(path,file);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children){
                importFiles(sqlCon, tableName,path,filename);
                disableTrigger(sqlCon,tableName);
                executeQuery(sqlCon,insertTmpTable(tableName,tableName+"_DEST","cbMarqSource,databaseSource",filename,0,0,"","","DE_No"));
                executeQuery(sqlCon,updateDepot());
                executeQuery(sqlCon,insertTable(tableName,tableName+"_TMP","cbMarqSource,databaseSource",filename,1,0,"DL_No","",""));
                executeQuery(sqlCon, updateTableDest("cbMarqSource,DatabaseSource", "DL_No", tableName, tableName + "_TMP", filename,0,0,""));
                enableTrigger(sqlCon,tableName);
            }
        }
    }

    public static void loadDeleteFile(String path,Connection sqlCon) {
        disableTrigger(sqlCon,tableName);
        loadDeleteFile(path,sqlCon,file,tableName,"cbMarq","DataBaseSource");
        enableTrigger(sqlCon,tableName);
    }

    public static void getDataElement(Connection sqlCon, String path, String database, String time, JSONObject type)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,type,""), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

    public static void getDataElementFilterAgency(Connection sqlCon, String path,String database,String time,String agency)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DatabaseSource");
        getData(sqlCon, selectSourceTableFilterAgencyEnteteLink(tableName,database,agency), tableName, path, filename);
        File avroFile = new File(path + "//" + filename);
        if (avroFile.exists())
            executeQuery(sqlCon,updateSelectTable(tableName,true));
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }
}
