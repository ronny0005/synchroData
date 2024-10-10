import java.sql.Connection;

public class Caisse extends Table {
    public static String file ="caisse_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_CAISSE";
    public static String configList = "listCaisse";

    public static void sendDataElement(Connection sqlCon, String path,String database,int unibase)
    {
        dbSource = database;
        loadFile(path,sqlCon);
        loadDeleteFile(path,sqlCon,file,tableName,"CA_No","DatabaseSource");
        //DocEntete.loadDeleteFile(path,sqlCon,file,tableName,deleteQuery());;
    }

    public static String updateDepotInsert(){

        return "UPDATE dest SET DE_No = ISNULL(dep.DE_No,dest.[DE_No])\n" +
                "FROM F_CAISSE_TMP dest\n" +
                "LEFT JOIN (SELECT DE_NoSource,DatabaseSource,DE_No FROM F_DEPOT) dep \n" +
                "ON ISNULL(dep.DE_NoSource,0) = ISNULL(dest.DE_No,0) \n" +
                "AND ISNULL(dep.DataBaseSource,'') = ISNULL(dest.DataBaseSource,'') " +
                "\n";
    }


    public static void loadFile(String path,Connection sqlCon){
        String [] children = getFile(path,file);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children){
                readOnFile(path, filename, tableName + "_DEST", sqlCon);

                executeQuery(sqlCon,insertTmpTable (tableName,tableName+"_DEST","CA_No,DatabaseSource",filename,0,1,"","CA_No","DE_No"));
                executeQuery(sqlCon,updateDepotInsert());
                executeQuery(sqlCon,insertTable (tableName,tableName+"_TMP","CA_No",filename,1,0,"CA_No","",""));
                //deleteTempTable(sqlCon, tableName+"_DEST");
            }
        }
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".avro";
        initTableParam(sqlCon,tableName,configList,"CA_No,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,true,"CA_No"), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

}
