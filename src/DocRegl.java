import org.json.simple.JSONObject;

import java.sql.Connection;

public class DocRegl extends Table {

    public static String file = "docRegl_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_DOCREGL";
    public static String configList = "listDocRegl";

    public static void sendDataElement(Connection sqlCon, String path,String database,int unibase)
    {
        dbSource = database;
        loadFile( path, sqlCon,unibase);
        loadDeleteFile(path,sqlCon);
//        DocEntete.loadDeleteFile(path,sqlCon);
    }

    public static String deleteEmptyDocEntete(){
        return "DELETE dest\n" +
                "FROM F_DOCREGL_DEST dest\n" +
                "LEFT JOIN F_DOCENTETE docE\n" +
                "ON docE.DO_Domaine = dest.DO_Domaine\n"+
                "AND docE.DO_Type = dest.DO_Type\n"+
                "AND docE.DO_Piece = dest.DO_Piece\n"+
                "WHERE docE.DO_Piece IS NULL";
    }
    public static void loadFile(String path,Connection sqlCon,int unibase){
        String [] children = getFile(path,file);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children){
                disableTrigger(sqlCon,tableName);
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                executeQuery(sqlCon, updateTableDest("", "'DR_No'", tableName, tableName + "_DEST",filename,unibase));

                executeQuery(sqlCon,insertTmpTable (tableName,tableName+"_DEST","cbMarqSource,DatabaseSource",filename,0,0,"","",""));
                executeQuery(sqlCon,deleteEmptyDocEntete());
                executeQuery(sqlCon,insertTable (tableName,tableName+"_TMP","cbMarqSource,DatabaseSource",filename,0,0,"","",""));
               // deleteTempTable(sqlCon, tableName+"_DEST");
                enableTrigger(sqlCon,tableName);

            }
        }
    }

    public static void loadDeleteFile(String path,Connection sqlCon) {
        disableTrigger(sqlCon,tableName);
        loadDeleteFile(path,sqlCon,file,tableName,"DR_No","DataBaseSource");
        enableTrigger(sqlCon,tableName);
    }

    public static void getDataElement(Connection sqlCon, String path, String database, String time, JSONObject type)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DR_No,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,type), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }
    public static void getDataElementFilterAgency(Connection sqlCon, String path,String database,String time,String agency)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DR_No,DatabaseSource");
        getData(sqlCon, selectSourceTableFilterAgencyEnteteLink(tableName,database,agency), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }

}
