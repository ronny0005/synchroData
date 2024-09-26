import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class Famille extends Table {

    public static String file ="famille_";
    public static String tableName = "F_FAMILLE";
    public static String configList = "listFamille";

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
                executeQuery(sqlCon, updateTableDest("FA_CodeFamille", "'FA_CodeFamille','FA_Type'", tableName, tableName + "_DEST", filename,unibase));
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","FA_CodeFamille",filename,0,0,"","",""));

                FamCompta.sendDataElement(sqlCon, path,unibase);
                deleteTempTable(sqlCon, tableName + "_DEST");

            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"","FA_CodeFamille");
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"FA_CodeFamille");
        getData(sqlCon, selectSourceTable(tableName,database,true), tableName, path, filename);

        FamCompta.getDataElement(sqlCon, path,database, time);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);


    }

}
