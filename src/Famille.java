import java.io.File;
import java.sql.Connection;

public class Famille extends Table {

    public static String file ="famille.csv";
    public static String tableName = "F_FAMILLE";
    public static String configList = "listFamille";

    public static String list()
    {
        return "SELECT [FA_CodeFamille],[FA_Type],[FA_Intitule],[FA_UniteVen],[FA_Coef],[FA_SuiviStock]\n" +
                "      ,[FA_Garantie],[FA_Central],[FA_Stat01],[FA_Stat02],[FA_Stat03],[FA_Stat04],[FA_Stat05]\n" +
                "      ,[FA_CodeFiscal],[FA_Pays],[FA_UnitePoids],[FA_Escompte],[FA_Delai],[FA_HorsStat],[FA_VteDebit]\n" +
                "      ,[FA_NotImp],[FA_Frais01FR_Denomination],[FA_Frais01FR_Rem01REM_Valeur],[FA_Frais01FR_Rem01REM_Type]\n" +
                "      ,[FA_Frais01FR_Rem02REM_Valeur],[FA_Frais01FR_Rem02REM_Type],[FA_Frais01FR_Rem03REM_Valeur]\n" +
                "      ,[FA_Frais01FR_Rem03REM_Type],[FA_Frais02FR_Denomination],[FA_Frais02FR_Rem01REM_Valeur]\n" +
                "      ,[FA_Frais02FR_Rem01REM_Type],[FA_Frais02FR_Rem02REM_Valeur],[FA_Frais02FR_Rem02REM_Type]\n" +
                "      ,[FA_Frais02FR_Rem03REM_Valeur],[FA_Frais02FR_Rem03REM_Type],[FA_Frais03FR_Denomination]\n" +
                "      ,[FA_Frais03FR_Rem01REM_Valeur],[FA_Frais03FR_Rem01REM_Type],[FA_Frais03FR_Rem02REM_Valeur]\n" +
                "      ,[FA_Frais03FR_Rem02REM_Type],[FA_Frais03FR_Rem03REM_Valeur],[FA_Frais03FR_Rem03REM_Type]\n" +
                "      ,[FA_Contremarque],[FA_FactPoids],[FA_FactForfait],[FA_Publie],[FA_RacineRef],[FA_RacineCB]\n" +
                "      ,[CL_No1],[CL_No2],[CL_No3],[CL_No4],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]\n" +
                "\t  ,cbMarqSource = [cbMarq]\n" +
                "  FROM [F_FAMILLE]\n" +
                "  WHERE cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='F_FAMILLE'),'1900-01-01')\n";
    }

    public static String insert()
    {
        return "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                " UPDATE F_FAMILLE_DEST   \n" +
                "SET  [FA_Frais01FR_Rem01REM_Valeur] = REPLACE(FA_Frais01FR_Rem01REM_Valeur,',','.')\n" +
                "      ,[FA_Frais01FR_Rem02REM_Valeur] = REPLACE(FA_Frais01FR_Rem02REM_Valeur,',','.')\n" +
                "      ,[FA_Frais01FR_Rem03REM_Valeur] = REPLACE(FA_Frais01FR_Rem03REM_Valeur,',','.')\n" +
                "      ,[FA_Frais02FR_Rem01REM_Valeur] = REPLACE(FA_Frais02FR_Rem01REM_Valeur,',','.')\n" +
                "      ,[FA_Frais02FR_Rem02REM_Valeur] = REPLACE(FA_Frais02FR_Rem02REM_Valeur,',','.')\n" +
                "      ,[FA_Frais02FR_Rem03REM_Valeur] = REPLACE(FA_Frais02FR_Rem03REM_Valeur,',','.')\n" +
                "      ,[FA_Frais03FR_Rem01REM_Valeur] = REPLACE(FA_Frais03FR_Rem01REM_Valeur,',','.')\n" +
                "      ,[FA_Frais03FR_Rem02REM_Valeur] = REPLACE(FA_Frais03FR_Rem02REM_Valeur,',','.')\n" +
                "      ,[FA_Frais03FR_Rem03REM_Valeur] = REPLACE(FA_Frais03FR_Rem03REM_Valeur,',','.')\n" +
                "\t\t\t\t\t\t  \n" +
                "UPDATE F_FAMILLE   \n" +
                "SET\t  [FA_Intitule] = LEFT(F_FAMILLE_DEST.FA_Intitule,35)\n" +
                "      ,[FA_UniteVen] = F_FAMILLE_DEST.FA_UniteVen\n" +
                "      ,[FA_Coef] = F_FAMILLE_DEST.FA_Coef\n" +
                "      ,[FA_SuiviStock] = F_FAMILLE_DEST.FA_SuiviStock\n" +
                "      ,[FA_Garantie] = F_FAMILLE_DEST.FA_Garantie\n" +
                "      ,[FA_Central] = F_FAMILLE_DEST.FA_Central\n" +
                "      ,[FA_Stat01] = F_FAMILLE_DEST.FA_Stat01\n" +
                "      ,[FA_Stat02] = F_FAMILLE_DEST.FA_Stat02\n" +
                "      ,[FA_Stat03] = F_FAMILLE_DEST.FA_Stat03\n" +
                "      ,[FA_Stat04] = F_FAMILLE_DEST.FA_Stat04\n" +
                "      ,[FA_Stat05] = F_FAMILLE_DEST.FA_Stat05\n" +
                "      ,[FA_CodeFiscal] = F_FAMILLE_DEST.FA_CodeFiscal\n" +
                "      ,[FA_Pays] = F_FAMILLE_DEST.FA_Pays\n" +
                "      ,[FA_UnitePoids] = F_FAMILLE_DEST.FA_UnitePoids\n" +
                "      ,[FA_Escompte] = F_FAMILLE_DEST.FA_Escompte\n" +
                "      ,[FA_Delai] = F_FAMILLE_DEST.FA_Delai\n" +
                "      ,[FA_HorsStat] = F_FAMILLE_DEST.FA_HorsStat\n" +
                "      ,[FA_VteDebit] = F_FAMILLE_DEST.FA_VteDebit\n" +
                "      ,[FA_NotImp] = F_FAMILLE_DEST.FA_NotImp\n" +
                "      ,[FA_Frais01FR_Denomination] = F_FAMILLE_DEST.FA_Frais01FR_Denomination\n" +
                "      ,[FA_Frais01FR_Rem01REM_Valeur] = F_FAMILLE_DEST.FA_Frais01FR_Rem01REM_Valeur\n" +
                "      ,[FA_Frais01FR_Rem01REM_Type] = F_FAMILLE_DEST.FA_Frais01FR_Rem01REM_Type\n" +
                "      ,[FA_Frais01FR_Rem02REM_Valeur] = F_FAMILLE_DEST.FA_Frais01FR_Rem02REM_Valeur\n" +
                "      ,[FA_Frais01FR_Rem02REM_Type] = F_FAMILLE_DEST.FA_Frais01FR_Rem02REM_Type\n" +
                "      ,[FA_Frais01FR_Rem03REM_Valeur] = F_FAMILLE_DEST.FA_Frais01FR_Rem03REM_Valeur\n" +
                "      ,[FA_Frais01FR_Rem03REM_Type] = F_FAMILLE_DEST.FA_Frais01FR_Rem03REM_Type\n" +
                "      ,[FA_Frais02FR_Denomination] = F_FAMILLE_DEST.FA_Frais02FR_Denomination\n" +
                "      ,[FA_Frais02FR_Rem01REM_Valeur] = F_FAMILLE_DEST.FA_Frais02FR_Rem01REM_Valeur\n" +
                "      ,[FA_Frais02FR_Rem01REM_Type] = F_FAMILLE_DEST.FA_Frais02FR_Rem01REM_Type\n" +
                "      ,[FA_Frais02FR_Rem02REM_Valeur] = F_FAMILLE_DEST.FA_Frais02FR_Rem02REM_Valeur\n" +
                "      ,[FA_Frais02FR_Rem02REM_Type] = F_FAMILLE_DEST.FA_Frais02FR_Rem02REM_Type\n" +
                "      ,[FA_Frais02FR_Rem03REM_Valeur] = F_FAMILLE_DEST.FA_Frais02FR_Rem03REM_Valeur\n" +
                "      ,[FA_Frais02FR_Rem03REM_Type] = F_FAMILLE_DEST.FA_Frais02FR_Rem03REM_Type\n" +
                "      ,[FA_Frais03FR_Denomination] = F_FAMILLE_DEST.FA_Frais03FR_Denomination\n" +
                "      ,[FA_Frais03FR_Rem01REM_Valeur] = F_FAMILLE_DEST.FA_Frais03FR_Rem01REM_Valeur\n" +
                "      ,[FA_Frais03FR_Rem01REM_Type] = F_FAMILLE_DEST.FA_Frais03FR_Rem01REM_Type\n" +
                "      ,[FA_Frais03FR_Rem02REM_Valeur] = F_FAMILLE_DEST.FA_Frais03FR_Rem02REM_Valeur\n" +
                "      ,[FA_Frais03FR_Rem02REM_Type] = F_FAMILLE_DEST.FA_Frais03FR_Rem02REM_Type\n" +
                "      ,[FA_Frais03FR_Rem03REM_Valeur] = F_FAMILLE_DEST.FA_Frais03FR_Rem03REM_Valeur\n" +
                "      ,[FA_Frais03FR_Rem03REM_Type] = F_FAMILLE_DEST.FA_Frais03FR_Rem03REM_Type\n" +
                "      ,[FA_Contremarque] = F_FAMILLE_DEST.FA_Contremarque\n" +
                "      ,[FA_FactPoids] = F_FAMILLE_DEST.FA_FactPoids\n" +
                "      ,[FA_FactForfait] = F_FAMILLE_DEST.FA_FactForfait\n" +
                "      ,[FA_Publie] = F_FAMILLE_DEST.FA_Publie\n" +
                "      ,[FA_RacineRef] = F_FAMILLE_DEST.FA_RacineRef\n" +
                "      ,[FA_RacineCB] = F_FAMILLE_DEST.FA_RacineCB\n" +
                "      ,[CL_No1] = F_FAMILLE_DEST.CL_No1\n" +
                "      ,[CL_No2] = F_FAMILLE_DEST.CL_No2\n" +
                "      ,[CL_No3] = F_FAMILLE_DEST.CL_No3\n" +
                "      ,[CL_No4] = F_FAMILLE_DEST.CL_No4\n" +
                "      ,[cbProt] = F_FAMILLE_DEST.cbProt\n" +
                "      ,[cbCreateur] = F_FAMILLE_DEST.cbCreateur\n" +
                "      ,[cbModification] = F_FAMILLE_DEST.cbModification\n" +
                "      ,[cbReplication] = F_FAMILLE_DEST.cbReplication\n" +
                "      ,[cbFlag] = F_FAMILLE_DEST.cbFlag \n" +
                "FROM F_FAMILLE_DEST  \n" +
                "WHERE F_FAMILLE.FA_CodeFamille = F_FAMILLE_DEST.FA_CodeFamille\n" +
                "\t\t\t\t \n" +
                "INSERT INTO [dbo].[F_FAMILLE]\n" +
                "    ([FA_CodeFamille],[FA_Type],[FA_Intitule],[FA_UniteVen],[FA_Coef],[FA_SuiviStock],[FA_Garantie],[FA_Central]\n" +
                "    ,[FA_Stat01],[FA_Stat02],[FA_Stat03],[FA_Stat04],[FA_Stat05],[FA_CodeFiscal],[FA_Pays],[FA_UnitePoids]\n" +
                "    ,[FA_Escompte],[FA_Delai],[FA_HorsStat],[FA_VteDebit],[FA_NotImp],[FA_Frais01FR_Denomination]\n" +
                "    ,[FA_Frais01FR_Rem01REM_Valeur],[FA_Frais01FR_Rem01REM_Type],[FA_Frais01FR_Rem02REM_Valeur],[FA_Frais01FR_Rem02REM_Type]\n" +
                "    ,[FA_Frais01FR_Rem03REM_Valeur],[FA_Frais01FR_Rem03REM_Type],[FA_Frais02FR_Denomination],[FA_Frais02FR_Rem01REM_Valeur]\n" +
                "    ,[FA_Frais02FR_Rem01REM_Type],[FA_Frais02FR_Rem02REM_Valeur],[FA_Frais02FR_Rem02REM_Type],[FA_Frais02FR_Rem03REM_Valeur]\n" +
                "    ,[FA_Frais02FR_Rem03REM_Type],[FA_Frais03FR_Denomination],[FA_Frais03FR_Rem01REM_Valeur],[FA_Frais03FR_Rem01REM_Type]\n" +
                "    ,[FA_Frais03FR_Rem02REM_Valeur],[FA_Frais03FR_Rem02REM_Type],[FA_Frais03FR_Rem03REM_Valeur],[FA_Frais03FR_Rem03REM_Type]\n" +
                "    ,[FA_Contremarque],[FA_FactPoids],[FA_FactForfait],[FA_Publie],[FA_RacineRef],[FA_RacineCB],[CL_No1],[CL_No2],[CL_No3],[CL_No4]\n" +
                "    ,[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag])\n" +
                "\t\t\t\t\t\t  \n" +
                "SELECT dest.[FA_CodeFamille],[FA_Type],[FA_Intitule],[FA_UniteVen],[FA_Coef],[FA_SuiviStock],[FA_Garantie],[FA_Central]\n" +
                "           ,[FA_Stat01],[FA_Stat02],[FA_Stat03],[FA_Stat04],[FA_Stat05],[FA_CodeFiscal],[FA_Pays],[FA_UnitePoids]\n" +
                "           ,[FA_Escompte],[FA_Delai],[FA_HorsStat],[FA_VteDebit],[FA_NotImp],[FA_Frais01FR_Denomination]\n" +
                "           ,[FA_Frais01FR_Rem01REM_Valeur],[FA_Frais01FR_Rem01REM_Type],[FA_Frais01FR_Rem02REM_Valeur],[FA_Frais01FR_Rem02REM_Type]\n" +
                "           ,[FA_Frais01FR_Rem03REM_Valeur],[FA_Frais01FR_Rem03REM_Type],[FA_Frais02FR_Denomination],[FA_Frais02FR_Rem01REM_Valeur]\n" +
                "           ,[FA_Frais02FR_Rem01REM_Type],[FA_Frais02FR_Rem02REM_Valeur],[FA_Frais02FR_Rem02REM_Type],[FA_Frais02FR_Rem03REM_Valeur]\n" +
                "           ,[FA_Frais02FR_Rem03REM_Type],[FA_Frais03FR_Denomination],[FA_Frais03FR_Rem01REM_Valeur],[FA_Frais03FR_Rem01REM_Type]\n" +
                "           ,[FA_Frais03FR_Rem02REM_Valeur],[FA_Frais03FR_Rem02REM_Type],[FA_Frais03FR_Rem03REM_Valeur],[FA_Frais03FR_Rem03REM_Type]\n" +
                "           ,[FA_Contremarque],[FA_FactPoids],[FA_FactForfait],[FA_Publie],[FA_RacineRef],[FA_RacineCB],[CL_No1],[CL_No2],[CL_No3],[CL_No4]\n" +
                "           ,[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]\n" +
                "FROM F_FAMILLE_DEST dest  \n" +
                "LEFT JOIN (SELECT [FA_CodeFamille] FROM F_FAMILLE) src  \n" +
                "ON dest.[FA_CodeFamille] = src.[FA_CodeFamille]  \n" +
                "WHERE src.[FA_CodeFamille] IS NULL ;\n" +
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
                "   'F_FAMILLE',\n" +
                "   GETDATE());\n" +
                "END CATCH";

    }

    public static void deleteTempTable(Connection sqlCon)
    {
        String query = "IF OBJECT_ID('F_FAMILLE_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_FAMILLE_DEST;";
        executeQuery(sqlCon, query);
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        /*readOnFile(path,file,"F_FAMILLE_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"F_FAMILLE_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());

         */
        FamCompta.sendDataElement(sqlCon, path,database);
        deleteTempTable(sqlCon);
        deleteFamille(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        /*initTable(sqlCon);
        getData(sqlCon, list(), "F_FAMILLE", path, file);

         */
        FamCompta.getDataElement(sqlCon, path,database);
        //listDeleteFamille(sqlCon, path);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);


    }
    public static void initTable(Connection sqlCon)
    {
        String query =  " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_FAMILLE') \n" +
                "     INSERT INTO config.ListFamille\n" +
                "     SELECT FA_CodeFamille,cbMarq \n" +
                "     FROM F_FAMILLE";
        executeQuery(sqlCon, query);
    }
    public static void deleteFamille(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_FAMILLE  \n" +
                " WHERE EXISTS (SELECT 1 FROM F_FAMILLE_SUPPR WHERE F_FAMILLE_SUPPR.FA_CodeFamille = F_FAMILLE.FA_CodeFamille)  \n" +
                "  \n" +
                " IF OBJECT_ID('F_FAMILLE_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_FAMILLE_SUPPR ;";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }

    public static void listDeleteFamille(Connection sqlCon, String path)
    {
        String query = " SELECT lart.FA_CodeFamille,lart.cbMarq " +
                " FROM config.ListFamille lart " +
                " LEFT JOIN dbo.F_FAMILLE fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\deleteList" + file, query, sqlCon);

        query = " DELETE FROM config.ListFamille " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_FAMILLE " +
                "                  WHERE dbo.F_FAMILLE.cbMarq = config.ListFamille.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
