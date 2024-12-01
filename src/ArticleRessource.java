import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class ArticleRessource extends Table {

    public static String file ="articleressource_";
    public static String tableName = "F_ARTICLERESSOURCE";
    public static String configList = "ListArticleRessource";
    public static String keyColumns = "RP_Code,AR_Ref";

    public static String list()
    {
        return "SELECT\t[AR_Ref],[RP_Code],[cbProt],[cbMarq]\n" +
                "\t\t,[cbCreateur],[cbModification],[cbReplication],[cbFlag]\n" +
                " ,cbMarqSource = cbMarq " +
                " FROM\t[F_ARTICLERESSOURCE] " +
                "WHERE cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='F_ARTICLERESSOURCE'),'1900-01-01')";
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
                executeQuery(sqlCon, updateTableDest(keyColumns, "RP_Code", tableName, tableName + "_DEST", filename,unibase,0,""));
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","AR_Ref,RP_Code",filename,0,0,"","",""));
            }
        }

        loadDeleteFile(path,sqlCon,file,tableName,"",keyColumns);
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,keyColumns);//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,database,true,""), tableName, path, filename);
        File avroFile = new File(path + "//" + filename);
        if (avroFile.exists())
            executeQuery(sqlCon,updateSelectTable(tableName,true));
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

}
