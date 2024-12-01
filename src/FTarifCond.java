import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class FTarifCond extends Table{

    public static String file = "ftarifcond_";
    public static String tableName = "F_TARIFCOND";
    public static String configList = "listFTarifCond";

    public static String updateCONoInsert(){

        return " UPDATE dest SET CO_No = ISNULL(con.CO_No,dest.[CO_No])\n" +
                " FROM F_TARIFCOND_DEST dest "+
                " LEFT JOIN (SELECT CO_NoSource,DatabaseSource,CO_No FROM F_CONDITION) con \n" +
                " ON ISNULL(con.CO_NoSource,0) = ISNULL(dest.CO_No,0) \n" +
                " AND ISNULL(con.DataBaseSource,'') = ISNULL(dest.DataBaseSource,'') " +
                "\n";
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
                importFiles(sqlCon, tableName,path,filename);
                executeQuery(sqlCon,updateCONoInsert());
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","AR_Ref,CO_No",filename,0,0,"","",""));
                executeQuery(sqlCon, updateTableDest("AR_Ref,CO_No", "AR_Ref,CO_No,TC_RefCF", tableName, tableName + "_DEST", filename,unibase,0,""));
            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"","AR_Ref,CO_No");
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"AR_Ref,CO_No");
        getData(sqlCon, selectSourceTable(tableName,database,true,""), tableName, path, filename);
        File avroFile = new File(path + "//" + filename);
        if (avroFile.exists())
            executeQuery(sqlCon,updateSelectTable(tableName,true));
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }
}
