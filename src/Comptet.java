import java.io.File;
import java.sql.Connection;

public class Comptet extends Table {

    public static String file = "comptet.csv";
    public static String tableName = "F_COMPTET";
    public static String configList = "listComptet";

    public static String insert()
    {
        return  "BEGIN TRY " +
                " \n" +
                "SET DATEFORMAT ymd;\n" +
                "INSERT INTO F_COMPTET (\n" +
                "[CT_Num],[CT_Intitule],[CT_Type],[CG_NumPrinc],[CT_Qualite],[CT_Classement]\n" +
                "      ,[CT_Contact],[CT_Adresse],[CT_Complement],[CT_CodePostal]\n" +
                "      ,[CT_Ville],[CT_CodeRegion],[CT_Pays],[CT_Raccourci]\n" +
                "      ,[BT_Num],[N_Devise],[CT_Ape],[CT_Identifiant]\n" +
                "      ,[CT_Siret],[CT_Statistique01],[CT_Statistique02],[CT_Statistique03]\n" +
                "      ,[CT_Statistique04],[CT_Statistique05],[CT_Statistique06],[CT_Statistique07]\n" +
                "      ,[CT_Statistique08],[CT_Statistique09],[CT_Statistique10],[CT_Commentaire]\n" +
                "      ,[CT_Encours],[CT_Assurance],[CT_NumPayeur],[N_Risque]\n" +
                "      ,[CO_No],[N_CatTarif],[CT_Taux01],[CT_Taux02],[CT_Taux03],[CT_Taux04]\n" +
                "      ,[N_CatCompta],[N_Period],[CT_Facture],[CT_BLFact]\n" +
                "      ,[CT_Langue],[CT_Edi1],[CT_Edi2],[CT_Edi3],[N_Expedition],[N_Condition]\n" +
                "      ,[CT_DateCreate],[CT_Saut],[CT_Lettrage],[CT_ValidEch]\n" +
                "      ,[CT_Sommeil],[DE_No],[CT_ControlEnc],[CT_NotRappel],[N_Analytique],[CA_Num],[CT_Telephone],[CT_Telecopie]\n" +
                "      ,[CT_EMail],[CT_Site],[CT_Coface],[CT_Surveillance],[CT_SvDateCreate],[CT_SvFormeJuri]\n" +
                "      ,[CT_SvEffectif],[CT_SvCA],[CT_SvResultat],[CT_SvIncident],[CT_SvDateIncid],[CT_SvPrivil]\n" +
                "      ,[CT_SvRegul],[CT_SvCotation],[CT_SvDateMaj],[CT_SvObjetMaj]\n" +
                "      ,[CT_SvDateBilan],[CT_SvNbMoisBilan],[N_AnalytiqueIFRS],[CA_NumIFRS]\n" +
                "      ,[CT_PrioriteLivr],[CT_LivrPartielle]\n" +
                "      ,[MR_No],[CT_NotPenal],[EB_No],[CT_NumCentrale],[CT_DateFermeDebut],[CT_DateFermeFin]\n" +
                "      ,[CT_FactureElec],[CT_TypeNIF],[CT_RepresentInt],[CT_RepresentNIF]\n" +
                "      ,[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag])\n" +
                "            \n" +
                "SELECT dest.[CT_Num],[CT_Intitule],[CT_Type],[CG_NumPrinc],LEFT([CT_Qualite],17),[CT_Classement]\n" +
                "      ,[CT_Contact],[CT_Adresse],[CT_Complement],[CT_CodePostal]\n" +
                "      ,[CT_Ville],[CT_CodeRegion],[CT_Pays],[CT_Raccourci]\n" +
                "      /*,[BT_Num]*/,0,[N_Devise],[CT_Ape],[CT_Identifiant]\n" +
                "      ,[CT_Siret],[CT_Statistique01],[CT_Statistique02],[CT_Statistique03]\n" +
                "      ,[CT_Statistique04],[CT_Statistique05],[CT_Statistique06],[CT_Statistique07]\n" +
                "      ,[CT_Statistique08],[CT_Statistique09],[CT_Statistique10],[CT_Commentaire]\n" +
                "      ,[CT_Encours],[CT_Assurance],[CT_NumPayeur],[N_Risque]\n" +
                "      ,[CO_No],[N_CatTarif],[CT_Taux01],[CT_Taux02],[CT_Taux03],[CT_Taux04]\n" +
                "      ,[N_CatCompta],[N_Period],[CT_Facture],[CT_BLFact]\n" +
                "      ,[CT_Langue],[CT_Edi1],[CT_Edi2],[CT_Edi3],[N_Expedition],[N_Condition]\n" +
                "      ,[CT_DateCreate],[CT_Saut],[CT_Lettrage],[CT_ValidEch]\n" +
                "      ,[CT_Sommeil],[DE_No],[CT_ControlEnc],[CT_NotRappel],[N_Analytique],[CA_Num],[CT_Telephone],[CT_Telecopie]\n" +
                "      ,[CT_EMail],[CT_Site],[CT_Coface],[CT_Surveillance],[CT_SvDateCreate],[CT_SvFormeJuri]\n" +
                "      ,[CT_SvEffectif],[CT_SvCA],[CT_SvResultat],[CT_SvIncident],[CT_SvDateIncid],[CT_SvPrivil]\n" +
                "      ,[CT_SvRegul],[CT_SvCotation],[CT_SvDateMaj],[CT_SvObjetMaj]\n" +
                "      ,[CT_SvDateBilan],[CT_SvNbMoisBilan],[N_AnalytiqueIFRS],[CA_NumIFRS]\n" +
                "      ,[CT_PrioriteLivr],[CT_LivrPartielle]\n" +
                "      ,[MR_No],[CT_NotPenal],[EB_No],[CT_NumCentrale],[CT_DateFermeDebut],[CT_DateFermeFin]\n" +
                "      ,[CT_FactureElec],[CT_TypeNIF],[CT_RepresentInt],[CT_RepresentNIF]\n" +
                "      ,[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]\n" +
                "FROM F_COMPTET_DEST dest\n" +
                "LEFT JOIN (SELECT CT_Num FROM F_COMPTET) src\n" +
                "\tON\tdest.CT_Num = src.CT_Num\n" +
                "WHERE src.CT_Num IS NULL\n" +
                "            \n" +
                "IF OBJECT_ID('F_COMPTET_DEST') IS NOT NULL \n" +
                "DROP TABLE F_COMPTET_DEST \n" +
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
                "   'insert',\n"+
                "   'F_COMPTET',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "CT_Num,CT_Type","'CT_Num','CT_Type'",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,insert());

        deleteTempTable(sqlCon,tableName);
        deleteComptet(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"CT_Num,CT_Type");
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList,database);
    }

    public static void deleteComptet(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_COMPTET \n" +
                " WHERE CT_Num IN(SELECT CT_Num FROM F_COMPTET_SUPPR) \n" +
                " \n" +
                " IF OBJECT_ID('F_COMPTET_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_COMPTET_SUPPR \n";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }

}
