import java.io.File;
import java.sql.Connection;

public class ArtClient extends Table{
    public static String file ="ArtClient.csv";
    public static String tableName = "F_ARTCLIENT";
    public static String configList = "listArtClient";

    public static String list()
    {
        return   " SELECT [AR_Ref],[AC_Categorie],[AC_PrixVen],[AC_Coef]\n" +
                "      ,[AC_PrixTTC],[AC_Arrondi],[AC_QteMont],[EG_Champ]\n" +
                "      ,[AC_PrixDev],[AC_Devise],[CT_Num],[AC_Remise]\n" +
                "      ,[AC_Calcul],[AC_TypeRem],[AC_RefClient],[AC_CoefNouv]\n" +
                "      ,[AC_PrixVenNouv],[AC_PrixDevNouv],[AC_RemiseNouv],[AC_DateApplication]\n" +
                "      ,[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]\n" +
                "\t    ,cbMarqSource = [cbMarq]\n" +
                "  FROM [F_ARTCLIENT]\n" +
                "  WHERE cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='F_ARTCLIENT'),'1900-01-01')";
    }

    public static String insert()
    {
        return
                        " BEGIN TRY " +
                        "SET DATEFORMAT ymd;\n" +
                        " UPDATE F_ARTCLIENT_DEST     \n" +
                        " SET  [AC_PrixVen] = REPLACE(AC_PrixVen,',','.')\n" +
                        "      ,[AC_Coef] = REPLACE(AC_Coef,',','.')\n" +
                        "      ,[AC_PrixDev] = REPLACE(AC_PrixDev,',','.')\n" +
                        "      ,[AC_Devise] = REPLACE(AC_Devise,',','.')\n" +
                        "      ,[AC_Remise] = REPLACE(AC_Remise,',','.')\n" +
                        "      ,[AC_CoefNouv] = REPLACE(AC_CoefNouv,',','.')\n" +
                        "      ,[AC_PrixVenNouv] = REPLACE(AC_PrixVenNouv,',','.')\n" +
                        "      ,[AC_PrixDevNouv] = REPLACE(AC_PrixDevNouv,',','.')\n" +
                        "      ,[AC_RemiseNouv] = REPLACE(AC_RemiseNouv,',','.')\n" +
                        "      \n" +
                        " UPDATE F_ARTCLIENT     \n" +
                        " SET  [AC_PrixVen] = [F_ARTCLIENT_DEST].AC_PrixVen\n" +
                        "      ,[AC_Coef] = [F_ARTCLIENT_DEST].AC_Coef\n" +
                        "      ,[AC_PrixTTC] = [F_ARTCLIENT_DEST].AC_PrixTTC\n" +
                        "      ,[AC_Arrondi] = [F_ARTCLIENT_DEST].AC_Arrondi\n" +
                        "      ,[AC_QteMont] = [F_ARTCLIENT_DEST].AC_QteMont\n" +
                        "      ,[EG_Champ] = [F_ARTCLIENT_DEST].EG_Champ\n" +
                        "      ,[AC_PrixDev] = [F_ARTCLIENT_DEST].AC_PrixDev\n" +
                        "      ,[AC_Devise] = [F_ARTCLIENT_DEST].AC_Devise\n" +
                        "      ,[AC_Remise] = [F_ARTCLIENT_DEST].AC_Remise\n" +
                        "      ,[AC_Calcul] = [F_ARTCLIENT_DEST].AC_Calcul\n" +
                        "      ,[AC_TypeRem] = [F_ARTCLIENT_DEST].AC_TypeRem\n" +
                        "      ,[AC_RefClient] = [F_ARTCLIENT_DEST].AC_RefClient\n" +
                        "      ,[AC_CoefNouv] = [F_ARTCLIENT_DEST].AC_CoefNouv\n" +
                        "      ,[AC_PrixVenNouv] = [F_ARTCLIENT_DEST].AC_PrixVenNouv\n" +
                        "      ,[AC_PrixDevNouv] = [F_ARTCLIENT_DEST].AC_PrixDevNouv\n" +
                        "      ,[AC_RemiseNouv] = [F_ARTCLIENT_DEST].AC_RemiseNouv\n" +
                        "      ,[AC_DateApplication] = [F_ARTCLIENT_DEST].AC_DateApplication\n" +
                        "      ,[cbProt] = [F_ARTCLIENT_DEST].cbProt\n" +
                        "      ,[cbCreateur] = [F_ARTCLIENT_DEST].cbCreateur\n" +
                        "      ,[cbModification] = [F_ARTCLIENT_DEST].cbModification\n" +
                        "      ,[cbReplication] = [F_ARTCLIENT_DEST].cbReplication\n" +
                        "      ,[cbFlag] = [F_ARTCLIENT_DEST].cbFlag\n" +
                        " FROM F_ARTCLIENT_DEST    \n" +
                        " WHERE F_ARTCLIENT.AR_Ref = F_ARTCLIENT_DEST.AR_Ref  \n" +
                        " AND F_ARTCLIENT.AC_Categorie = F_ARTCLIENT_DEST.AC_Categorie\n" +
                        "  \n" +
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
                        "   'F_ARTCLIENT',\n" +
                        "   GETDATE());\n" +
                        "END CATCH";
    }

    public static void deleteTempTable(Connection sqlCon)
    {
        String query = "IF OBJECT_ID('F_ARTCLIENT_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_ARTCLIENT_DEST;";
        executeQuery(sqlCon, query);
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {
        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
//        sendData(sqlCon, path, file, insert());

        deleteTempTable(sqlCon);
        deleteArtClient(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);

        //initTable(sqlCon);
        //getData(sqlCon, list(), "F_ARTCLIENT", path, file);
//        listDeleteArtClient(sqlCon, path);
    }
    public static void initTable(Connection sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_ARTCLIENT') \n" +
                "     INSERT INTO config.ListArtClient\n" +
                "     SELECT AR_Ref,AC_Categorie,cbMarq \n" +
                "     FROM F_ARTCLIENT";
        executeQuery(sqlCon, query);
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

    public static void listDeleteArtClient(Connection sqlCon, String path)
    {
        String query = " SELECT lart.AR_Ref,lart.AC_Categorie,lart.cbMarq " +
                " FROM config.ListArtClient lart " +
                " LEFT JOIN dbo.F_ARTCLIENT fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\deleteList" + file, query, sqlCon);

        query = " DELETE FROM config.ListArtClient " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_ARTCLIENT " +
                "                  WHERE dbo.F_ARTCLIENT.cbMarq = config.ListArtClient.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
