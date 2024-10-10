import org.json.simple.JSONObject;

import java.sql.Connection;

public class ReglEch extends Table {

    public static String file = "ReglEch_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_REGLECH";
    public static String configList = "listReglEch";

    public static String linkDrRGNo (){
        return
                "\n" +
                "DELETE FROM F_REGLECH_DEST \n" +
                "WHERE cbMarqSource IN (SELECT dest.cbMarqSource\n" +
                "FROM F_REGLECH_DEST dest\n" +
                "LEFT JOIN F_DOCREGL fdr\n" +
                "ON ISNULL(fdr.DataBaseSource,'') = ISNULL(dest.DataBaseSource,'')\n" +
                "AND ISNULL(fdr.DR_NoSource,0) = ISNULL(dest.DR_No,0)\n" +
                "LEFT JOIN F_CREGLEMENT cre\n" +
                "ON ISNULL(cre.DataBaseSource,'') = ISNULL(dest.DataBaseSource,'')\n" +
                "AND ISNULL(cre.RG_NoSource,0) = ISNULL(dest.RG_No,0)\n" +
                "WHERE cre.RG_No IS NULL OR fdr.DR_No IS NULL);\n"+
                "UPDATE dest SET RG_No = cre.[RG_No]\n" +
                "\t\t\t\t,DR_No = fdr.[DR_No]\n" +
                "FROM F_REGLECH_DEST dest\n" +
                "LEFT JOIN F_DOCREGL fdr\n" +
                "ON ISNULL(fdr.DataBaseSource,'') = ISNULL(dest.DataBaseSource,'')\n" +
                "AND ISNULL(fdr.DR_NoSource,0) = ISNULL(dest.DR_No,0)\n" +
                "LEFT JOIN F_CREGLEMENT cre\n" +
                "ON ISNULL(cre.DataBaseSource,'') = ISNULL(dest.DataBaseSource,'')\n" +
                "AND ISNULL(cre.RG_NoSource,0) = ISNULL(dest.RG_No,0)\n" +
                "WHERE cre.RG_No IS NOT NULL AND fdr.DR_No IS NOT NULL;\n" +
                "\n";
    }
    public static void sendDataElement(Connection sqlCon, String path,String database,int unibase)
    {
        dbSource = database;
        loadFile(path,sqlCon,unibase);
        disableTrigger(sqlCon,tableName);
        loadDeleteFile(path,sqlCon,file,tableName,"cbMarq","dataBaseSource");
        enableTrigger(sqlCon,tableName);

    }

    public static void loadFile(String path,Connection sqlCon,int unibase){
        String [] children = getFile(path,file);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                disableTrigger(sqlCon,tableName);
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                executeQuery(sqlCon, updateTableDest("", "'RG_No','DR_No'", tableName, tableName + "_DEST",filename,unibase));
                executeQuery(sqlCon,linkDrRGNo());
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","cbMarqSource,dataBaseSource",filename,0,1,"","cbMarqSource,DR_NoSource",""));
                enableTrigger(sqlCon,tableName);

            }
        }
    }

    public static void getDataElement(Connection sqlCon, String path, String database, String time, JSONObject type)
    {
        String filename =  file+time+".avro";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DR_No,RG_No,DataBaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,type,"DR_No,RG_No"), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }
    public static void getDataElementFilterAgency(Connection sqlCon, String path,String database,String time,String agency)
    {
        String filename =  file+time+".avro";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DR_No,RG_No,DataBaseSource");
        getData(sqlCon, selectSourceTableFilterAgencyEnteteLink(tableName,database,agency), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }

    public static void deleteTempTable(Connection sqlCon)
    {
        String query = "IF OBJECT_ID('F_REGLECH_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_REGLECH_DEST;";
        executeQuery(sqlCon, query);
    }
}
