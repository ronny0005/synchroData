import java.io.File;
import java.sql.Connection;

public class Article extends Table {
    public static String file = "article.csv";
    public static String tableName = "F_ARTICLE";
    public static String configList = "listArticle";

    public static String list()
    {
        return " SELECT " +
                " [AR_Ref],[AR_Design],[FA_CodeFamille],[AR_Substitut],[AR_Raccourci] " +
                ",[AR_Garantie],[AR_UnitePoids],[AR_PoidsNet],[AR_PoidsBrut],[AR_UniteVen] " +
                ",[AR_PrixAch],[AR_Coef],[AR_PrixVen],[AR_PrixTTC],[AR_Gamme1],[AR_Gamme2] " +
                ",[AR_SuiviStock],[AR_Nomencl],[AR_Stat01],[AR_Stat02],[AR_Stat03],[AR_Stat04],[AR_Stat05] " +
                ",[AR_Escompte],[AR_Delai],[AR_HorsStat],[AR_VteDebit],[AR_NotImp],[AR_Sommeil],[AR_Langue1],[AR_Langue2],[AR_CodeEdiED_Code1] " +
                ",[AR_CodeEdiED_Code2],[AR_CodeEdiED_Code3],[AR_CodeEdiED_Code4],[AR_CodeBarre],[AR_CodeFiscal],[AR_Pays] " +
                ",[AR_Frais01FR_Denomination],[AR_Frais01FR_Rem01REM_Valeur],[AR_Frais01FR_Rem01REM_Type],[AR_Frais01FR_Rem02REM_Valeur] " +
                ",[AR_Frais01FR_Rem02REM_Type],[AR_Frais01FR_Rem03REM_Valeur],[AR_Frais01FR_Rem03REM_Type],[AR_Frais02FR_Denomination] " +
                ",[AR_Frais02FR_Rem01REM_Valeur],[AR_Frais02FR_Rem01REM_Type],[AR_Frais02FR_Rem02REM_Valeur],[AR_Frais02FR_Rem02REM_Type] " +
                ",[AR_Frais02FR_Rem03REM_Valeur],[AR_Frais02FR_Rem03REM_Type],[AR_Frais03FR_Denomination],[AR_Frais03FR_Rem01REM_Valeur] " +
                ",[AR_Frais03FR_Rem01REM_Type],[AR_Frais03FR_Rem02REM_Valeur],[AR_Frais03FR_Rem02REM_Type],[AR_Frais03FR_Rem03REM_Valeur] " +
                ",[AR_Frais03FR_Rem03REM_Type],[AR_Condition],[AR_PUNet],[AR_Contremarque],[AR_FactPoids],[AR_FactForfait],[AR_DateCreation],[AR_SaisieVar] " +
                ",[AR_Transfere],[AR_Publie],[AR_DateModif],[AR_Photo],[AR_PrixAchNouv],[AR_CoefNouv],[AR_PrixVenNouv],[AR_DateApplication] " +
                ",[AR_CoutStd],[AR_QteComp],[AR_QteOperatoire],[CO_No],[AR_Prevision],[CL_No1],[CL_No2],[CL_No3] " +
                ",[CL_No4],[AR_Type],[RP_CodeDefaut],[cbProt],[cbCreateur] " +
                ",[cbReplication],[cbFlag],cbModification, cbMarqSource = cbMarq" +
                " FROM F_ARTICLE " +
                " WHERE cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='F_ARTICLE'),'1900-01-01') ";
    }

    public static String insert()
    {
        return  "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                "UPDATE F_ARTICLE \n" +
                "\tSET [AR_Design] = LEFT(F_ARTICLE_DEST.AR_Design,69),[FA_CodeFamille] = F_ARTICLE_DEST.FA_CodeFamille,[AR_Substitut] = F_ARTICLE_DEST.AR_Substitut,[AR_Raccourci] = F_ARTICLE_DEST.AR_Raccourci\n" +
                "      ,[AR_Garantie] = F_ARTICLE_DEST.AR_Garantie,[AR_UnitePoids] = F_ARTICLE_DEST.AR_UnitePoids,[AR_PoidsNet] = F_ARTICLE_DEST.AR_PoidsNet,[AR_PoidsBrut] = F_ARTICLE_DEST.AR_PoidsBrut\n" +
                "      ,[AR_UniteVen] = F_ARTICLE_DEST.AR_UniteVen,[AR_PrixAch] = F_ARTICLE_DEST.AR_PrixAch,[AR_Coef] = F_ARTICLE_DEST.AR_Coef,[AR_PrixVen] = F_ARTICLE_DEST.AR_PrixVen\n" +
                "      ,[AR_PrixTTC] = F_ARTICLE_DEST.AR_PrixTTC,[AR_Gamme1] = F_ARTICLE_DEST.AR_Gamme1,[AR_Gamme2] = F_ARTICLE_DEST.AR_Gamme2/*,[AR_SuiviStock] = F_ARTICLE_DEST.AR_SuiviStock*/,[AR_Nomencl] = F_ARTICLE_DEST.AR_Nomencl,[AR_Stat01] = F_ARTICLE_DEST.AR_Stat01\n" +
                "      ,[AR_Stat02] = F_ARTICLE_DEST.AR_Stat02,[AR_Stat03] = F_ARTICLE_DEST.AR_Stat03,[AR_Stat04] = F_ARTICLE_DEST.AR_Stat04,[AR_Stat05] = F_ARTICLE_DEST.AR_Stat05\n" +
                "      ,[AR_Escompte] = F_ARTICLE_DEST.AR_Escompte,[AR_Delai] = F_ARTICLE_DEST.AR_Delai,[AR_HorsStat] = F_ARTICLE_DEST.AR_HorsStat,[AR_VteDebit] = F_ARTICLE_DEST.AR_VteDebit\n" +
                "      ,[AR_NotImp] = F_ARTICLE_DEST.AR_NotImp,[AR_Sommeil] = F_ARTICLE_DEST.AR_Sommeil,[AR_Langue1] = F_ARTICLE_DEST.AR_Langue1,[AR_Langue2] = F_ARTICLE_DEST.AR_Langue2\n" +
                "      ,[AR_CodeEdiED_Code1] = F_ARTICLE_DEST.AR_CodeEdiED_Code1,[AR_CodeEdiED_Code2] = F_ARTICLE_DEST.AR_CodeEdiED_Code2\n" +
                "      ,[AR_CodeEdiED_Code3] = F_ARTICLE_DEST.AR_CodeEdiED_Code3,[AR_CodeEdiED_Code4] = F_ARTICLE_DEST.AR_CodeEdiED_Code4\n" +
                "      ,[AR_CodeBarre] = F_ARTICLE_DEST.AR_CodeBarre,[AR_CodeFiscal] = F_ARTICLE_DEST.AR_CodeFiscal,[AR_Pays] = F_ARTICLE_DEST.AR_Pays,[AR_Frais01FR_Denomination] = F_ARTICLE_DEST.AR_Frais01FR_Denomination\n" +
                "      ,[AR_Frais01FR_Rem01REM_Valeur] = F_ARTICLE_DEST.AR_Frais01FR_Rem01REM_Valeur,[AR_Frais01FR_Rem01REM_Type] = F_ARTICLE_DEST.AR_Frais01FR_Rem01REM_Type\n" +
                "      ,[AR_Frais01FR_Rem02REM_Valeur] = F_ARTICLE_DEST.AR_Frais01FR_Rem02REM_Valeur,[AR_Frais01FR_Rem02REM_Type] = F_ARTICLE_DEST.AR_Frais01FR_Rem02REM_Type\n" +
                "      ,[AR_Frais01FR_Rem03REM_Valeur] = F_ARTICLE_DEST.AR_Frais01FR_Rem03REM_Valeur,[AR_Frais01FR_Rem03REM_Type] = F_ARTICLE_DEST.AR_Frais01FR_Rem03REM_Type\n" +
                "      ,[AR_Frais02FR_Denomination] = F_ARTICLE_DEST.AR_Frais02FR_Denomination,[AR_Frais02FR_Rem01REM_Valeur] = F_ARTICLE_DEST.AR_Frais02FR_Rem01REM_Valeur\n" +
                "      ,[AR_Frais02FR_Rem01REM_Type] = F_ARTICLE_DEST.AR_Frais02FR_Rem01REM_Type,[AR_Frais02FR_Rem02REM_Valeur] = F_ARTICLE_DEST.AR_Frais02FR_Rem02REM_Valeur\n" +
                "      ,[AR_Frais02FR_Rem02REM_Type] = F_ARTICLE_DEST.AR_Frais02FR_Rem02REM_Type,[AR_Frais02FR_Rem03REM_Valeur] = F_ARTICLE_DEST.AR_Frais02FR_Rem03REM_Valeur\n" +
                "      ,[AR_Frais02FR_Rem03REM_Type] = F_ARTICLE_DEST.AR_Frais02FR_Rem03REM_Type,[AR_Frais03FR_Denomination] = F_ARTICLE_DEST.AR_Frais03FR_Denomination\n" +
                "      ,[AR_Frais03FR_Rem01REM_Valeur] = F_ARTICLE_DEST.AR_Frais03FR_Rem01REM_Valeur,[AR_Frais03FR_Rem01REM_Type] = F_ARTICLE_DEST.AR_Frais03FR_Rem01REM_Type\n" +
                "      ,[AR_Frais03FR_Rem02REM_Valeur] = F_ARTICLE_DEST.AR_Frais03FR_Rem02REM_Valeur,[AR_Frais03FR_Rem02REM_Type] = F_ARTICLE_DEST.AR_Frais03FR_Rem02REM_Type\n" +
                "      ,[AR_Frais03FR_Rem03REM_Valeur] = F_ARTICLE_DEST.AR_Frais03FR_Rem03REM_Valeur,[AR_Frais03FR_Rem03REM_Type] = F_ARTICLE_DEST.AR_Frais03FR_Rem03REM_Type\n" +
                "      /*,[AR_Condition] = F_ARTICLE_DEST.AR_Condition*/,[AR_PUNet] = F_ARTICLE_DEST.AR_PUNet,[AR_Contremarque] = F_ARTICLE_DEST.AR_Contremarque,[AR_FactPoids] = F_ARTICLE_DEST.AR_FactPoids\n" +
                "      ,[AR_FactForfait] = F_ARTICLE_DEST.AR_FactForfait,[AR_DateCreation] = F_ARTICLE_DEST.AR_DateCreation,[AR_SaisieVar] = F_ARTICLE_DEST.AR_SaisieVar,[AR_Transfere] = F_ARTICLE_DEST.AR_Transfere\n" +
                "      ,[AR_Publie] = F_ARTICLE_DEST.AR_Publie,[AR_DateModif] = F_ARTICLE_DEST.AR_DateModif,[AR_Photo] = F_ARTICLE_DEST.AR_Photo,[AR_PrixAchNouv] = F_ARTICLE_DEST.AR_PrixAchNouv\n" +
                "      ,[AR_CoefNouv] = F_ARTICLE_DEST.AR_CoefNouv,[AR_PrixVenNouv] = F_ARTICLE_DEST.AR_PrixVenNouv,[AR_DateApplication] = F_ARTICLE_DEST.AR_DateApplication,[AR_CoutStd] = F_ARTICLE_DEST.AR_CoutStd\n" +
                "      ,[AR_QteComp] = F_ARTICLE_DEST.AR_QteComp,[AR_QteOperatoire] = F_ARTICLE_DEST.AR_QteOperatoire,[CO_No] = F_ARTICLE_DEST.CO_No,[AR_Prevision] = F_ARTICLE_DEST.AR_Prevision\n" +
                "      ,[CL_No1] = F_ARTICLE_DEST.CL_No1,[CL_No2] = F_ARTICLE_DEST.CL_No2,[CL_No3] = F_ARTICLE_DEST.CL_No3,[CL_No4] = F_ARTICLE_DEST.CL_No4\n" +
                "      ,[AR_Type] = F_ARTICLE_DEST.AR_Type/*,[RP_CodeDefaut] = F_ARTICLE_DEST.RP_CodeDefaut*/,[cbProt] = F_ARTICLE_DEST.cbProt,[cbCreateur] = F_ARTICLE_DEST.cbCreateur\n" +
                "      ,[cbModification] = F_ARTICLE_DEST.cbModification,[cbReplication] = F_ARTICLE_DEST.cbReplication,[cbFlag] = F_ARTICLE_DEST.cbFlag\n" +
                "FROM F_ARTICLE_DEST\n" +
                "WHERE F_ARTICLE.AR_Ref = F_ARTICLE_DEST.AR_Ref\n" +
                "\n" +
                "INSERT INTO F_ARTICLE (\n" +
                "[AR_Ref],[AR_Design],[FA_CodeFamille],[AR_Substitut],[AR_Raccourci] \n" +
                ",[AR_Garantie],[AR_UnitePoids],[AR_PoidsNet],[AR_PoidsBrut],[AR_UniteVen] \n" +
                ",[AR_PrixAch],[AR_Coef],[AR_PrixVen],[AR_PrixTTC],[AR_Gamme1],[AR_Gamme2] \n" +
                ",[AR_SuiviStock],[AR_Nomencl],[AR_Stat01],[AR_Stat02],[AR_Stat03],[AR_Stat04],[AR_Stat05] \n" +
                ",[AR_Escompte],[AR_Delai],[AR_HorsStat],[AR_VteDebit],[AR_NotImp],[AR_Sommeil],[AR_Langue1],[AR_Langue2],[AR_CodeEdiED_Code1] \n" +
                ",[AR_CodeEdiED_Code2],[AR_CodeEdiED_Code3],[AR_CodeEdiED_Code4],[AR_CodeBarre],[AR_CodeFiscal],[AR_Pays] \n" +
                ",[AR_Frais01FR_Denomination],[AR_Frais01FR_Rem01REM_Valeur],[AR_Frais01FR_Rem01REM_Type],[AR_Frais01FR_Rem02REM_Valeur] \n" +
                ",[AR_Frais01FR_Rem02REM_Type],[AR_Frais01FR_Rem03REM_Valeur],[AR_Frais01FR_Rem03REM_Type],[AR_Frais02FR_Denomination] \n" +
                ",[AR_Frais02FR_Rem01REM_Valeur],[AR_Frais02FR_Rem01REM_Type],[AR_Frais02FR_Rem02REM_Valeur],[AR_Frais02FR_Rem02REM_Type] \n" +
                ",[AR_Frais02FR_Rem03REM_Valeur],[AR_Frais02FR_Rem03REM_Type],[AR_Frais03FR_Denomination],[AR_Frais03FR_Rem01REM_Valeur] \n" +
                ",[AR_Frais03FR_Rem01REM_Type],[AR_Frais03FR_Rem02REM_Valeur],[AR_Frais03FR_Rem02REM_Type],[AR_Frais03FR_Rem03REM_Valeur] \n" +
                ",[AR_Frais03FR_Rem03REM_Type],[AR_Condition],[AR_PUNet],[AR_Contremarque],[AR_FactPoids],[AR_FactForfait],[AR_DateCreation],[AR_SaisieVar] \n" +
                ",[AR_Transfere],[AR_Publie],[AR_DateModif],[AR_Photo],[AR_PrixAchNouv],[AR_CoefNouv],[AR_PrixVenNouv],[AR_DateApplication] \n" +
                ",[AR_CoutStd],[AR_QteComp],[AR_QteOperatoire],[CO_No],[AR_Prevision],[CL_No1],[CL_No2],[CL_No3] \n" +
                ",[CL_No4],[AR_Type],[RP_CodeDefaut],[cbProt],[cbCreateur] \n" +
                ",[cbReplication],[cbFlag])\n" +
                "\n" +
                "SELECT dest.[AR_Ref],LEFT(dest.[AR_Design],69),dest.[FA_CodeFamille],dest.[AR_Substitut],dest.[AR_Raccourci] \n" +
                ",dest.[AR_Garantie],dest.[AR_UnitePoids],dest.[AR_PoidsNet],dest.[AR_PoidsBrut],dest.[AR_UniteVen] \n" +
                ",dest.[AR_PrixAch],dest.[AR_Coef],dest.[AR_PrixVen],dest.[AR_PrixTTC],dest.[AR_Gamme1],dest.[AR_Gamme2] \n" +
                ",dest.[AR_SuiviStock],dest.[AR_Nomencl],dest.[AR_Stat01],dest.[AR_Stat02],dest.[AR_Stat03],dest.[AR_Stat04],dest.[AR_Stat05] \n" +
                ",dest.[AR_Escompte],dest.[AR_Delai],dest.[AR_HorsStat],dest.[AR_VteDebit],dest.[AR_NotImp],dest.[AR_Sommeil],dest.cbMarqSource/*dest.[AR_Langue1]*/,dest.[AR_Langue2],dest.[AR_CodeEdiED_Code1] \n" +
                ",dest.[AR_CodeEdiED_Code2],dest.[AR_CodeEdiED_Code3],dest.[AR_CodeEdiED_Code4],dest.[AR_CodeBarre],dest.[AR_CodeFiscal],dest.[AR_Pays] \n" +
                ",dest.[AR_Frais01FR_Denomination],dest.[AR_Frais01FR_Rem01REM_Valeur],dest.[AR_Frais01FR_Rem01REM_Type],dest.[AR_Frais01FR_Rem02REM_Valeur] \n" +
                ",dest.[AR_Frais01FR_Rem02REM_Type],dest.[AR_Frais01FR_Rem03REM_Valeur],dest.[AR_Frais01FR_Rem03REM_Type],dest.[AR_Frais02FR_Denomination] \n" +
                ",dest.[AR_Frais02FR_Rem01REM_Valeur],dest.[AR_Frais02FR_Rem01REM_Type],dest.[AR_Frais02FR_Rem02REM_Valeur],dest.[AR_Frais02FR_Rem02REM_Type] \n" +
                ",dest.[AR_Frais02FR_Rem03REM_Valeur],dest.[AR_Frais02FR_Rem03REM_Type],dest.[AR_Frais03FR_Denomination],dest.[AR_Frais03FR_Rem01REM_Valeur] \n" +
                ",dest.[AR_Frais03FR_Rem01REM_Type],dest.[AR_Frais03FR_Rem02REM_Valeur],dest.[AR_Frais03FR_Rem02REM_Type],dest.[AR_Frais03FR_Rem03REM_Valeur] \n" +
                ",dest.[AR_Frais03FR_Rem03REM_Type],dest.[AR_Condition],dest.[AR_PUNet],dest.[AR_Contremarque],dest.[AR_FactPoids],dest.[AR_FactForfait],dest.[AR_DateCreation],dest.[AR_SaisieVar] \n" +
                ",dest.[AR_Transfere],dest.[AR_Publie],dest.[AR_DateModif],dest.[AR_Photo],dest.[AR_PrixAchNouv],dest.[AR_CoefNouv],dest.[AR_PrixVenNouv],dest.[AR_DateApplication] \n" +
                ",dest.[AR_CoutStd],dest.[AR_QteComp],dest.[AR_QteOperatoire],null/*dest.[CO_No]*/,dest.[AR_Prevision],dest.[CL_No1],dest.[CL_No2],dest.[CL_No3] \n" +
                ",dest.[CL_No4],dest.[AR_Type],null/*dest.[RP_CodeDefaut]*/,dest.[cbProt],dest.[cbCreateur],dest.[cbReplication],dest.[cbFlag]\n" +
                "FROM F_ARTICLE_DEST dest\n" +
                "LEFT JOIN F_ARTICLE src\n" +
                "\tON dest.AR_Ref = src.AR_Ref\n" +
                "WHERE src.AR_Ref IS NULL\n" +
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
                "   'F_ARTICLE',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }

    public static void deleteTempTable(Connection sqlCon)
    {
        String query = "IF OBJECT_ID('F_ARTICLE_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_ARTICLE_DEST;"
                + "\tIF OBJECT_ID('F_ARTICLERESSOURCE_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_ARTICLERESSOURCE_DEST;"+
                "\tIF OBJECT_ID('F_CONDITION_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_CONDITION_DEST;" +
                "\tIF OBJECT_ID('F_RESSOURCEPROD_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_RESSOURCEPROD_DEST";
        executeQuery(sqlCon, query);
    }

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

    public static void sendDataElement(Connection sqlCon, String path,String database)
    {
        readOnFile(path,file,tableName+"_DEST",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        Condition.sendDataElement(sqlCon, path,database);
        RessourceProd.sendDataElement(sqlCon, path,database);
        ArticleRessource.sendDataElement(sqlCon, path,database);
        ArtCompta.sendDataElement(sqlCon, path,database);
        ArtClient.sendDataElement(sqlCon, path,database);
        ArtFourniss.sendDataElement(sqlCon, path,database);
        linkArticle(sqlCon);
        readOnFile(path,"deleteList"+file,"F_ARTCLIENT_SUPPR",sqlCon);
        deleteTempTable(sqlCon);
        deleteItem(sqlCon, path,file,tableName);
//        deleteArticle(sqlCon, path);
    }

    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"AR_Ref");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
//        listDeleteArticle(sqlCon, path, "deleteList" + file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        Condition.getDataElement(sqlCon, path,database);
        ArticleRessource.getDataElement(sqlCon, path,database);
        RessourceProd.getDataElement(sqlCon, path,database);
        ArtCompta.getDataElement(sqlCon, path,database);
        ArtClient.getDataElement(sqlCon, path,database);
        ArtFourniss.getDataElement(sqlCon, path,database);
    }

    public static void listDeleteArticle(Connection sqlCon, String path, String file)
    {
        String query = " SELECT lart.AR_Ref,lart.cbMarq " +
                " FROM config.ListArticle lart " +
                " LEFT JOIN dbo.F_ARTICLE fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\" + file, query, sqlCon);

        query = " DELETE FROM config.ListArticle " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_ARTICLE " +
                "                  WHERE dbo.F_ARTICLE.cbMarq = config.ListArticle.cbMarq);";
        executeQuery(sqlCon, query);
    }

    public static void deleteArticle(Connection sqlCon,String path)
    {
        String query = "IF OBJECT_ID('F_ARTICLE_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_ARTICLE_SUPPR \n" +
                " \n" +
                " CREATE TABLE F_ARTICLE_SUPPR(AR_Ref NVARCHAR(50), cbMarq INT); \n" +
                " \n" +
                "             SET DATEFORMAT dmy; \n" +
                "             BULK INSERT  F_ARTICLE_SUPPR \n" +
                "             FROM '"+path+ "\\deleteList" + file+ "' \n" +
                " WITH \n" +
                " ( \n" +
                "     FIRSTROW = 2, \n" +
                "     FIELDTERMINATOR = ';', --CSV field delimiter  \n" +
                "     ROWTERMINATOR = '\n', --Use to shift the control to next row  \n" +
                "     TABLOCK \n" +
                " ) \n" +
                " \n" +
                " DELETE FROM F_ARTICLE \n" +
                " WHERE AR_Ref IN(SELECT AR_Ref FROM F_ARTICLE_SUPPR) \n" +
                " \n" +
                " IF OBJECT_ID('F_ARTICLE_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_ARTICLE_SUPPR \n";
        if ((new File(path + "\\deleteList" + file)).exists()) {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }
    public static void initTable(Connection sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_ARTICLE') "+
                " INSERT INTO config.ListArticle " +
                " SELECT AR_Ref,cbMarq " +
                " FROM F_ARTICLE " +
                " ELSE " +
                "    BEGIN " +
                "        INSERT INTO config.ListArticle" +
                " SELECT AR_Ref,cbMarq " +
                " FROM F_ARTICLE " +
                " WHERE cbMarq > (SELECT Max(cbMarq) FROM config.ListArticle)" +
                "END ";
        executeQuery(sqlCon, query);
    }
}
