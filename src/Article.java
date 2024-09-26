import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class Article extends Table {
    public static String file = "article_";
    public static String tableName = "F_ARTICLE";
    public static String configList = "listArticle";

    public static void linkArticle(Connection sqlCon)
    {
        String query = "UPDATE F_ARTICLE \n" +
                "     SET CO_No = F_ARTICLE_DEST.CO_No \n" +
                "         , RP_CodeDefaut = F_ARTICLE_DEST.RP_CodeDefaut \n" +
                " FROM F_ARTICLE_DEST \n" +
                " WHERE F_ARTICLE_DEST.AR_Ref = F_ARTICLE.AR_Ref \n" +
                " UPDATE F_RESSOURCEPROD \n" +
                "     SET AR_RefDefaut = F_RESSOURCEPROD_DEST.AR_RefDefaut \n"+
                " FROM F_RESSOURCEPROD_DEST \n"+
                " WHERE F_RESSOURCEPROD_DEST.RP_Code = F_RESSOURCEPROD.RP_Code";
        executeQuery(sqlCon, query);
    }

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
                executeQuery(sqlCon, updateTableDest("AR_Ref", "'AR_Ref','AR_SuiviStock'", tableName, tableName + "_DEST", filename,unibase));
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","AR_Ref",filename,0,0,"","",""));
                Condition.sendDataElement(sqlCon, path,unibase);
                RessourceProd.sendDataElement(sqlCon, path,unibase);
                ArticleRessource.sendDataElement(sqlCon, path,unibase);
                ArtCompta.sendDataElement(sqlCon, path,unibase);
                ArtClient.sendDataElement(sqlCon, path,unibase);
                ArtFourniss.sendDataElement(sqlCon, path,unibase);
                linkArticle(sqlCon);


            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"","AR_Ref");
    }

    public static void getDataElementFilterAgency(Connection sqlCon, String path,String database,String time,String agency)
    {

        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"AR_Ref");//initTable(sqlCon);
        getData(sqlCon, selectSourceTableFilterAgencyArticle(tableName,database,agency), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
        Condition.getDataElement(sqlCon, path,database, time);
        ArticleRessource.getDataElement(sqlCon, path,database,time);
        RessourceProd.getDataElement(sqlCon, path,database, time);
        ArtCompta.getDataElement(sqlCon, path,database, time);
        ArtClient.getDataElement(sqlCon, path,database,time);
        ArtFourniss.getDataElement(sqlCon, path,database,time);

        DepotEmpl.getDataElement(sqlCon, path,database, time);
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"AR_Ref");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,database,true), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
        Condition.getDataElement(sqlCon, path,database, time);
        ArticleRessource.getDataElement(sqlCon, path,database,time);
        RessourceProd.getDataElement(sqlCon, path,database, time);
        ArtCompta.getDataElement(sqlCon, path,database, time);
        ArtClient.getDataElement(sqlCon, path,database,time);
        ArtFourniss.getDataElement(sqlCon, path,database,time);
    }

}
