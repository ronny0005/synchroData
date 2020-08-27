import java.io.File;
import java.sql.Connection;

public class ArtClient extends Table{
    public static String file ="ArtClient.csv";
    public static String tableName = "F_ARTCLIENT";
    public static String configList = "listArtClient";

    public static String insert()
    {
        return
                        " BEGIN TRY " +
                        "SET DATEFORMAT ymd;\n" +
                        " INSERT INTO [dbo].[F_ARTCLIENT]  \n" +
                        " ([AR_Ref],[AC_Categorie],[AC_PrixVen],[AC_Coef]\n" +
                        "      ,[AC_PrixTTC],[AC_Arrondi],[AC_QteMont],[EG_Champ]\n" +
                        "      ,[AC_PrixDev],[AC_Devise],[CT_Num],[AC_Remise]\n" +
                        "      ,[AC_Calcul],[AC_TypeRem],[AC_RefClient],[AC_CoefNouv]\n" +
                        "      ,[AC_PrixVenNouv],[AC_PrixDevNouv],[AC_RemiseNouv],[AC_DateApplication]\n" +
                        "      ,[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag])  \n" +
                        " \n" +
                        " SELECT dest.[AR_Ref],dest.[AC_Categorie],[AC_PrixVen],[AC_Coef]\n" +
                        "      ,[AC_PrixTTC],[AC_Arrondi],[AC_QteMont],[EG_Champ]\n" +
                        "      ,[AC_PrixDev],[AC_Devise],[CT_Num],[AC_Remise]\n" +
                        "      ,[AC_Calcul],[AC_TypeRem],[AC_RefClient],[AC_CoefNouv]\n" +
                        "      ,[AC_PrixVenNouv],[AC_PrixDevNouv],[AC_RemiseNouv],[AC_DateApplication]\n" +
                        "      ,[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]\n" +
                        " FROM F_ARTCLIENT_DEST dest    \n" +
                        " LEFT JOIN (SELECT [AR_Ref],[AC_Categorie] FROM F_ARTCLIENT) src    \n" +
                        " ON dest.[AR_Ref] = src.[AR_Ref]    \n" +
                        " AND dest.[AC_Categorie] = src.[AC_Categorie]\n" +
                        " WHERE src.[AR_Ref] IS NULL ;  \n" +
                        " IF OBJECT_ID('F_ARTCLIENT_DEST') IS NOT NULL  \n" +
                        " DROP TABLE F_ARTCLIENT_DEST; \n" +
                        " END TRY\n" +
                        " BEGIN CATCH \n" +
                        "INSERT INTO config.DB_Errors\n" +
                        "    VALUES\n" +
                        "  (SUSER_SNAME(),\n" +
                        "   ERROR_NUMBER(),\n" +
                        "   ERROR_STATE(),\n" +
                        "   ERROR_SEVERITY(),\n" +
                        "   ERROR_LINE(),\n" +
                        "   ERROR_PROCEDURE(),\n" +
                        "   ERROR_MESSAGE(),\n" +
                        "   'Insert',\n" +
                        "   'F_ARTCLIENT',\n" +
                        "   GETDATE());\n" +
                        "END CATCH";
    }


    public static void sendDataElement(Connection sqlCon, String path,String database)
    {
        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref,AC_Categorie","'AR_Ref','AC_Categorie','CT_Num'",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,insert());

        deleteTempTable(sqlCon,tableName);
        deleteArtClient(sqlCon, path);
    }

    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList,database);
    }

    public static void deleteArtClient(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_ARTCLIENT  \n" +
                " WHERE EXISTS (SELECT 1 FROM F_ARTCLIENT_SUPPR WHERE F_ARTCLIENT_SUPPR.AR_Ref = F_ARTCLIENT.AR_Ref" +
                "   AND F_ARTCLIENT_SUPPR.AC_Categorie = F_ARTCLIENT.AC_Categorie )  \n" +
                "  \n" +
                " IF OBJECT_ID('F_ARTCLIENT_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_ARTCLIENT_SUPPR ;";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }
}
