import org.json.simple.JSONObject;

import java.sql.Connection;

public class DocEntete extends Table {
    public static String file = "docEntete_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_DOCENTETE";
    public static String configList = "listDocEntete";

    public static void sendDataElement(Connection sqlCon, String path,String database,int unibase)
    {
        dbSource = database;
        loadFile(path,sqlCon);
    }

    public static String updateDepotInsert() {

        return "UPDATE dest SET DE_No = ISNULL(dep.DE_No,dest.[DE_No])\n" +
                "               ,LI_No = ISNULL(liv.[LI_No],dest.[LI_No])\n" +
                "               ,CA_No = ISNULL(cai.CA_No,dest.[CA_No])\n" +
                "FROM F_DOCENTETE_TMP dest\n" +
                "LEFT JOIN F_DEPOT dep ON ISNULL(dep.DE_NoSource,0) = ISNULL(dest.DE_No,0) AND ISNULL(dep.dataBaseSource,'') = ISNULL(dest.dataBaseSource,'')   \n" +
                "LEFT JOIN F_CAISSE cai ON ISNULL(cai.CA_NoSource,0) = ISNULL(dest.CA_No,0) AND ISNULL(cai.dataBaseSource,'') = ISNULL(dest.dataBaseSource,'')   \n" +
                "LEFT JOIN F_LIVRAISON liv ON ISNULL(liv.LI_NoSource,0) = ISNULL(dest.LI_No,0) AND ISNULL(liv.dataBaseSource,'') = ISNULL(dest.dataBaseSource,'')   \n" +
                "\n";
    }

    public static void loadFile(String path,Connection sqlCon){
        String [] children = getFile(path,file);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                disableTrigger(sqlCon,tableName);
                executeQuery(sqlCon,insertTmpTable (tableName,tableName+"_DEST","DO_Piece,DO_Domaine,DO_Type",filename,0,0,"","","DE_No,CA_No,LI_No"));
                executeQuery(sqlCon,updateDepotInsert());
                executeQuery(sqlCon,insertTable (tableName,tableName+"_TMP","DO_Piece,DO_Domaine,DO_Type",filename,0,0,"","",""));
                enableTrigger(sqlCon,tableName);

            }
        }

        disableTrigger(sqlCon,tableName);
        loadDeleteFile(path,sqlCon,file,tableName,"cbMarq","DatabaseSource");
        enableTrigger(sqlCon,tableName);

    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time,JSONObject type)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,type), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

    public static void getDataElementFilterAgency(Connection sqlCon, String path,String database,String time,String agency)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DatabaseSource");
        getData(sqlCon, selectSourceTableFilterAgency(tableName,database,agency,"DE_No"), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }
}
