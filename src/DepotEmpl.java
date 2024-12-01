import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class DepotEmpl extends Table {

    public static String file = "depotEmpl_";
    public static String tableName = "F_DEPOTEMPL";
    public static String configList = "listDepotEmpl";

    public static String updateDepotInsert(){
        return "UPDATE tmp SET DE_No = ISNULL(srcDep.DE_No,dest.[DE_No])\n" +
                "FROM F_DEPOTEMPL_TMP tmp\n" +
                "INNER JOIN F_DEPOTEMPL_DEST dest ON tmp.cbMarqSource = dest.cbMarqSource\n"+
                "LEFT JOIN (SELECT DatabaseSource,DE_NoSource,DE_No FROM F_DEPOT) srcDep\n" +
                "ON ISNULL(dest.DE_No,0) = ISNULL(srcDep.DE_NoSource,0)\n" +
                "AND ISNULL(dest.DataBaseSource,'') = ISNULL(srcDep.DataBaseSource,'')\n" +
                "\n" +
                "DELETE \n" +
                "FROM F_DEPOTEMPL_TMP\n" +
                "WHERE DE_No IS NOT NULL";
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
                executeQuery(sqlCon,insertTmpTable (tableName,tableName+"_DEST","DP_No,DatabaseSource",filename,0,1,"","DP_No","DE_No"));
                executeQuery(sqlCon,updateDepotInsert());
                executeQuery(sqlCon,insertTable (tableName,tableName+"_TMP","DP_No",filename,1,0,"DP_No","",""));
                executeQuery(sqlCon, updateTableDest("DP_No,DatabaseSource", "DP_No,DE_No,DP_NoSource", tableName, tableName + "_TMP", filename,unibase,1,"DP_No"));
            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"DP_No","DataBaseSource");

    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"DP_No,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,true,"DP_No"), tableName, path, filename);
        File avroFile = new File(path + "//" + filename);
        if (avroFile.exists())
            executeQuery(sqlCon,updateSelectTable(tableName,true));
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }
}
