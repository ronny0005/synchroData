import java.io.File;
import java.sql.Connection;

public class ArtFourniss extends Table {

    public static String file ="ArtFourniss.csv";
    public static String tableName = "F_ARTFOURNISS";
    public static String configList = "listArtFourniss";
    public static String list()
    {
        return "SELECT [AR_Ref],[CT_Num],[AF_RefFourniss],[AF_PrixAch],[AF_Unite],[AF_Conversion],[AF_DelaiAppro]\n" +
                "      ,[AF_Garantie],[AF_Colisage],[AF_QteMini],[AF_QteMont],[EG_Champ],[AF_Principal]\n" +
                "      ,[AF_PrixDev],[AF_Devise],[AF_Remise],[AF_ConvDiv],[AF_TypeRem],[AF_CodeBarre]\n" +
                "      ,[AF_PrixAchNouv],[AF_PrixDevNouv],[AF_RemiseNouv],[AF_DateApplication],[cbProt],[cbMarq],[cbCreateur]\n" +
                "      ,[cbModification],[cbReplication],[cbFlag]\n" +
                "\t    ,cbMarqSource = [cbMarq]\n" +
                "  FROM [F_ARTFOURNISS]\n" +
                "  WHERE cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='F_ARTFOURNISS'),'1900-01-01')";
    }

    public static String insert()
    {
        return
                        "  BEGIN TRY " +
                        " SET DATEFORMAT ymd;\n" +
                        "  UPDATE F_ARTFOURNISS_DEST      \n" +
                        "  SET  [AF_PrixAch] = REPLACE([AF_PrixAch],',','.') \n" +
                        "\t   ,[AF_Conversion] = REPLACE([AF_Conversion],',','.') \n" +
                        "\t   ,[AF_Colisage] = REPLACE([AF_Colisage],',','.') \n" +
                        "\t   ,[AF_QteMini] = REPLACE([AF_QteMini],',','.') \n" +
                        "\t   ,[AF_PrixDev] = REPLACE([AF_PrixDev],',','.') \n" +
                        "\t   ,[AF_Remise] = REPLACE([AF_Remise],',','.') \n" +
                        "\t   ,[AF_ConvDiv] = REPLACE([AF_ConvDiv],',','.') \n" +
                        "\t   ,[AF_PrixAchNouv] = REPLACE([AF_PrixAchNouv],',','.') \n" +
                        "\t   ,[AF_PrixDevNouv] = REPLACE([AF_PrixDevNouv],',','.') \n" +
                        "\t   ,[AF_RemiseNouv] = REPLACE([AF_RemiseNouv],',','.') \n" +
                        "\t\t\n" +
                        "UPDATE [dbo].[F_ARTFOURNISS]\n" +
                        "   SET [AF_RefFourniss] = F_ARTFOURNISS_DEST.AF_RefFourniss\n" +
                        "      ,[AF_PrixAch] = F_ARTFOURNISS_DEST.AF_PrixAch\n" +
                        "      ,[AF_Unite] = F_ARTFOURNISS_DEST.AF_Unite\n" +
                        "      ,[AF_Conversion] = F_ARTFOURNISS_DEST.AF_Conversion\n" +
                        "      ,[AF_DelaiAppro] = F_ARTFOURNISS_DEST.AF_DelaiAppro\n" +
                        "      ,[AF_Garantie] = F_ARTFOURNISS_DEST.AF_Garantie\n" +
                        "      ,[AF_Colisage] = F_ARTFOURNISS_DEST.AF_Colisage\n" +
                        "      ,[AF_QteMini] = F_ARTFOURNISS_DEST.AF_QteMini\n" +
                        "      ,[AF_QteMont] = F_ARTFOURNISS_DEST.AF_QteMont\n" +
                        "      ,[EG_Champ] = F_ARTFOURNISS_DEST.EG_Champ\n" +
                        "      ,[AF_Principal] = F_ARTFOURNISS_DEST.AF_Principal\n" +
                        "      ,[AF_PrixDev] = F_ARTFOURNISS_DEST.AF_PrixDev\n" +
                        "      ,[AF_Devise] = F_ARTFOURNISS_DEST.AF_Devise\n" +
                        "      ,[AF_Remise] = F_ARTFOURNISS_DEST.AF_Remise\n" +
                        "      ,[AF_ConvDiv] = F_ARTFOURNISS_DEST.AF_ConvDiv\n" +
                        "      ,[AF_TypeRem] = F_ARTFOURNISS_DEST.AF_TypeRem\n" +
                        "      ,[AF_CodeBarre] = F_ARTFOURNISS_DEST.AF_CodeBarre\n" +
                        "      ,[AF_PrixAchNouv] = F_ARTFOURNISS_DEST.AF_PrixAchNouv\n" +
                        "      ,[AF_PrixDevNouv] = F_ARTFOURNISS_DEST.AF_PrixDevNouv\n" +
                        "      ,[AF_RemiseNouv] = F_ARTFOURNISS_DEST.AF_RemiseNouv\n" +
                        "      ,[AF_DateApplication] = F_ARTFOURNISS_DEST.AF_DateApplication\n" +
                        "      ,[cbProt] = F_ARTFOURNISS_DEST.cbProt\n" +
                        "      ,[cbCreateur] = F_ARTFOURNISS_DEST.cbCreateur\n" +
                        "      ,[cbModification] = F_ARTFOURNISS_DEST.cbModification\n" +
                        "      ,[cbReplication] = F_ARTFOURNISS_DEST.cbReplication\n" +
                        "      ,[cbFlag] = F_ARTFOURNISS_DEST.cbFlag\n" +
                        "FROM F_ARTFOURNISS_DEST     \n" +
                        "  WHERE F_ARTFOURNISS.AR_Ref = F_ARTFOURNISS_DEST.AR_Ref   \n" +
                        "  AND F_ARTFOURNISS.CT_Num = F_ARTFOURNISS_DEST.CT_Num\n" +
                        "\t\n" +
                        "  INSERT INTO [dbo].[F_ARTFOURNISS]   \n" +
                        "  ([AR_Ref],[CT_Num],[AF_RefFourniss],[AF_PrixAch],[AF_Unite],[AF_Conversion],[AF_DelaiAppro]\n" +
                        "      ,[AF_Garantie],[AF_Colisage],[AF_QteMini],[AF_QteMont],[EG_Champ],[AF_Principal]\n" +
                        "      ,[AF_PrixDev],[AF_Devise],[AF_Remise],[AF_ConvDiv],[AF_TypeRem],[AF_CodeBarre]\n" +
                        "      ,[AF_PrixAchNouv],[AF_PrixDevNouv],[AF_RemiseNouv],[AF_DateApplication],[cbProt],[cbCreateur]\n" +
                        "      ,[cbModification],[cbReplication],[cbFlag])   \n" +
                        "   \n" +
                        "  SELECT dest.[AR_Ref],dest.[CT_Num],[AF_RefFourniss],[AF_PrixAch],[AF_Unite],[AF_Conversion],[AF_DelaiAppro]\n" +
                        "      ,[AF_Garantie],[AF_Colisage],[AF_QteMini],[AF_QteMont],[EG_Champ],[AF_Principal]\n" +
                        "      ,[AF_PrixDev],[AF_Devise],[AF_Remise],[AF_ConvDiv],[AF_TypeRem],[AF_CodeBarre]\n" +
                        "      ,[AF_PrixAchNouv],[AF_PrixDevNouv],[AF_RemiseNouv],[AF_DateApplication],[cbProt],[cbCreateur]\n" +
                        "      ,[cbModification],[cbReplication],[cbFlag] \n" +
                        "  FROM F_ARTFOURNISS_DEST dest     \n" +
                        "  LEFT JOIN (SELECT [AR_Ref],[CT_Num] FROM F_ARTFOURNISS) src     \n" +
                        "  ON dest.[AR_Ref] = src.[AR_Ref]     \n" +
                        "  AND dest.[CT_Num] = src.[CT_Num] \n" +
                        "  WHERE src.[AR_Ref] IS NULL ;   \n" +
                        "  IF OBJECT_ID('F_ARTFOURNISS_DEST') IS NOT NULL   \n" +
                        "  DROP TABLE F_ARTFOURNISS_DEST\n" +
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
                        "   'F_ARTFOURNISS',\n" +
                        "   GETDATE());\n" +
                        "END CATCH";
    }

    public static void deleteTempTable(Connection sqlCon)
    {
        String query = "IF OBJECT_ID('F_ARTFOURNISS_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_ARTFOURNISS_DEST;";
        executeQuery(sqlCon, query);
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        /*readOnFile(path,file,"F_ARTFOURNISS_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"F_ARTFOURNISS_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());
        */deleteTempTable(sqlCon);
        deleteArtFourniss(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        /*initTable(sqlCon);
        getData(sqlCon, list(), "F_ARTFOURNISS", path, file);
        listDeleteArtFourniss(sqlCon, path);*/
    }
    public static void initTable(Connection sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_ARTFOURNISS') \n" +
                "     INSERT INTO config.ListArtFourniss\n" +
                "     SELECT AR_Ref,CT_Num,cbMarq \n" +
                "     FROM F_ARTFOURNISS";
        executeQuery(sqlCon, query);
    }
    public static void deleteArtFourniss(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_ARTFOURNISS  \n" +
                " WHERE EXISTS (SELECT 1 FROM F_ARTFOURNISS_SUPPR WHERE F_ARTFOURNISS_SUPPR.AR_Ref = F_ARTFOURNISS.AR_Ref AND F_ARTFOURNISS_SUPPR.CT_Num = F_ARTFOURNISS.CT_Num" +
                "   )  \n" +
                "  \n" +
                " IF OBJECT_ID('F_ARTFOURNISS_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_ARTFOURNISS_SUPPR ;";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }

    public static void listDeleteArtFourniss(Connection sqlCon, String path)
    {
        String query = " SELECT lart.AR_Ref,lart.CT_Num,lart.cbMarq " +
                " FROM config.ListArtFourniss lart " +
                " LEFT JOIN dbo.F_ARTFOURNISS fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\deleteList" + file, query, sqlCon);

        query = " DELETE FROM config.ListArtFourniss " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_ARTFOURNISS " +
                "                  WHERE dbo.F_ARTFOURNISS.cbMarq = config.ListArtFourniss.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
