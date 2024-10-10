import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class ArtFourniss extends Table {

    public static String file ="ArtFourniss_";
    public static String tableName = "F_ARTFOURNISS";
    public static String configList = "listArtFourniss";

    public static void sendDataElement(Connection sqlCon, String path,int unibase)
    {
        File dir = new File(path);
        FilenameFilter filter = (dir1, name) -> name.startsWith(file);
        String[] children = dir.list(filter);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                executeQuery(sqlCon, updateTableDest("AR_Ref,CT_Num", "'AR_Ref','CT_Num'", tableName, tableName + "_DEST", filename,unibase));
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","AR_Ref,CT_Num",filename,0,0,"","",""));

                deleteTempTable(sqlCon, tableName + "_DEST");

            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"","AR_Ref,CT_Num");
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".avro";
        initTableParam(sqlCon,tableName,configList,"AR_Ref,CT_Num");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,database,true,"")/*list()*/, tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

}
