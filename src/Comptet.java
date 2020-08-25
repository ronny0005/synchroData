import java.io.File;
import java.sql.Connection;

public class Comptet extends Table {

    public static String file = "comptet.csv";
    public static String tableName = "F_COMPTET";
    public static String configList = "listComptet";

    public static String list()
    {
        return  "SELECT [CT_Num],[CT_Intitule],[CT_Type],[CG_NumPrinc],[CT_Qualite],[CT_Classement]\n" +
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
                "      ,[cbProt],[cbMarq],[cbCreateur],[cbModification],[cbReplication],[cbFlag]\n" +
                "\t  ,cbMarqSource = cbMarq\n" +
                "FROM  [F_COMPTET]\n" +
                "WHERE cbModification>= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName = 'F_COMPTET'),'1900-01-01')";
    }

    public static String insert()
    {
        return  "BEGIN TRY " +
                " \n" +
                "SET DATEFORMAT ymd;\n" +
                "UPDATE\tF_COMPTET_DEST\n" +
                "SET\t[CT_Encours] = REPLACE([CT_Encours],',','.')\n" +
                "\t,[CT_Assurance] = REPLACE([CT_Assurance],',','.')\n" +
                "\t, [CT_Taux01] = REPLACE([CT_Taux01],',','.')\n" +
                "\t, [CT_Taux02] = REPLACE([CT_Taux02],',','.')\n" +
                "\t, [CT_Taux03] = REPLACE([CT_Taux03],',','.')\n" +
                "\t, [CT_Taux04] = REPLACE([CT_Taux04],',','.')\n" +
                "\t, [CT_SvCA] = REPLACE([CT_SvCA],',','.')\n" +
                "\t, [CT_SvResultat] = REPLACE([CT_SvResultat],',','.')\n" +
                "\n" +
                "UPDATE [dbo].[F_COMPTET]\n" +
                "   SET [CT_Intitule] = F_COMPTET_DEST.CT_Intitule\n" +
                "      ,[CG_NumPrinc] = F_COMPTET_DEST.CG_NumPrinc\n" +
                "      ,[CT_Qualite] = LEFT(F_COMPTET_DEST.[CT_Qualite],17)\n" +
                "      ,[CT_Classement] = F_COMPTET_DEST.CT_Classement\n" +
                "      ,[CT_Contact] = F_COMPTET_DEST.CT_Contact\n" +
                "      ,[CT_Adresse] = F_COMPTET_DEST.CT_Adresse\n" +
                "      ,[CT_Complement] = F_COMPTET_DEST.CT_Complement\n" +
                "      ,[CT_CodePostal] = F_COMPTET_DEST.CT_CodePostal\n" +
                "      ,[CT_Ville] = F_COMPTET_DEST.CT_Ville\n" +
                "      ,[CT_CodeRegion] = F_COMPTET_DEST.CT_CodeRegion\n" +
                "      ,[CT_Pays] = F_COMPTET_DEST.CT_Pays\n" +
                "      ,[CT_Raccourci] = F_COMPTET_DEST.CT_Raccourci\n" +
                "      /*,[BT_Num] = F_COMPTET_DEST.BT_Num*/\n" +
                "      ,[N_Devise] = F_COMPTET_DEST.N_Devise\n" +
                "      ,[CT_Ape] = F_COMPTET_DEST.CT_Ape\n" +
                "      ,[CT_Identifiant] = F_COMPTET_DEST.CT_Identifiant\n" +
                "      ,[CT_Siret] = F_COMPTET_DEST.CT_Siret\n" +
                "      ,[CT_Statistique01] = F_COMPTET_DEST.CT_Statistique01\n" +
                "      ,[CT_Statistique02] = F_COMPTET_DEST.CT_Statistique02\n" +
                "      ,[CT_Statistique03] = F_COMPTET_DEST.CT_Statistique03\n" +
                "      ,[CT_Statistique04] = F_COMPTET_DEST.CT_Statistique04\n" +
                "      ,[CT_Statistique05] = F_COMPTET_DEST.CT_Statistique05\n" +
                "      ,[CT_Statistique06] = F_COMPTET_DEST.CT_Statistique06\n" +
                "      ,[CT_Statistique07] = F_COMPTET_DEST.CT_Statistique07\n" +
                "      ,[CT_Statistique08] = F_COMPTET_DEST.CT_Statistique08\n" +
                "      ,[CT_Statistique09] = F_COMPTET_DEST.CT_Statistique09\n" +
                "      ,[CT_Statistique10] = F_COMPTET_DEST.CT_Statistique10\n" +
                "      ,[CT_Commentaire] = F_COMPTET_DEST.CT_Commentaire\n" +
                "      ,[CT_Encours] = F_COMPTET_DEST.CT_Encours\n" +
                "      ,[CT_Assurance] = F_COMPTET_DEST.CT_Assurance\n" +
                "      ,[CT_NumPayeur] = F_COMPTET_DEST.CT_NumPayeur\n" +
                "      ,[N_Risque] = F_COMPTET_DEST.N_Risque\n" +
                "      ,[CO_No] = F_COMPTET_DEST.CO_No\n" +
                "      ,[N_CatTarif] = F_COMPTET_DEST.N_CatTarif\n" +
                "      ,[CT_Taux01] = F_COMPTET_DEST.CT_Taux01\n" +
                "      ,[CT_Taux02] = F_COMPTET_DEST.CT_Taux02\n" +
                "      ,[CT_Taux03] = F_COMPTET_DEST.CT_Taux03\n" +
                "      ,[CT_Taux04] = F_COMPTET_DEST.CT_Taux04\n" +
                "      ,[N_CatCompta] = F_COMPTET_DEST.N_CatCompta\n" +
                "      ,[N_Period] = F_COMPTET_DEST.N_Period\n" +
                "      ,[CT_Facture] = F_COMPTET_DEST.CT_Facture\n" +
                "      ,[CT_BLFact] = F_COMPTET_DEST.CT_BLFact\n" +
                "      ,[CT_Langue] = F_COMPTET_DEST.CT_Langue\n" +
                "      ,[CT_Edi1] = F_COMPTET_DEST.CT_Edi1\n" +
                "      ,[CT_Edi2] = F_COMPTET_DEST.CT_Edi2\n" +
                "      ,[CT_Edi3] = F_COMPTET_DEST.CT_Edi3\n" +
                "      ,[N_Expedition] = F_COMPTET_DEST.N_Expedition\n" +
                "      ,[N_Condition] = F_COMPTET_DEST.N_Condition\n" +
                "      ,[CT_DateCreate] = F_COMPTET_DEST.CT_DateCreate\n" +
                "      ,[CT_Saut] = F_COMPTET_DEST.CT_Saut\n" +
                "      ,[CT_Lettrage] = F_COMPTET_DEST.CT_Lettrage\n" +
                "      ,[CT_ValidEch] = F_COMPTET_DEST.CT_ValidEch\n" +
                "      ,[CT_Sommeil] = F_COMPTET_DEST.CT_Sommeil\n" +
                "      ,[DE_No] = F_COMPTET_DEST.DE_No\n" +
                "      ,[CT_ControlEnc] = F_COMPTET_DEST.CT_ControlEnc\n" +
                "      ,[CT_NotRappel] = F_COMPTET_DEST.CT_NotRappel\n" +
                "      ,[N_Analytique] = F_COMPTET_DEST.N_Analytique\n" +
                "      ,[CA_Num] = F_COMPTET_DEST.CA_Num\n" +
                "      ,[CT_Telephone] = F_COMPTET_DEST.CT_Telephone\n" +
                "      ,[CT_Telecopie] = F_COMPTET_DEST.CT_Telecopie\n" +
                "      ,[CT_EMail] = F_COMPTET_DEST.CT_EMail\n" +
                "      ,[CT_Site] = F_COMPTET_DEST.CT_Site\n" +
                "      ,[CT_Coface] = F_COMPTET_DEST.CT_Coface\n" +
                "      ,[CT_Surveillance] = F_COMPTET_DEST.CT_Surveillance\n" +
                "      ,[CT_SvDateCreate] = F_COMPTET_DEST.CT_SvDateCreate\n" +
                "      ,[CT_SvFormeJuri] = F_COMPTET_DEST.CT_SvFormeJuri\n" +
                "      ,[CT_SvEffectif] = F_COMPTET_DEST.CT_SvEffectif\n" +
                "      ,[CT_SvCA] = F_COMPTET_DEST.CT_SvCA\n" +
                "      ,[CT_SvResultat] = F_COMPTET_DEST.CT_SvResultat\n" +
                "      ,[CT_SvIncident] = F_COMPTET_DEST.CT_SvIncident\n" +
                "      ,[CT_SvDateIncid] = F_COMPTET_DEST.CT_SvDateIncid\n" +
                "      ,[CT_SvPrivil] = F_COMPTET_DEST.CT_SvPrivil\n" +
                "      ,[CT_SvRegul] = F_COMPTET_DEST.CT_SvRegul\n" +
                "      ,[CT_SvCotation] = F_COMPTET_DEST.CT_SvCotation\n" +
                "      ,[CT_SvDateMaj] = F_COMPTET_DEST.CT_SvDateMaj\n" +
                "      ,[CT_SvObjetMaj] = F_COMPTET_DEST.CT_SvObjetMaj\n" +
                "      ,[CT_SvDateBilan] = F_COMPTET_DEST.CT_SvDateBilan\n" +
                "      ,[CT_SvNbMoisBilan] = F_COMPTET_DEST.CT_SvNbMoisBilan\n" +
                "      ,[N_AnalytiqueIFRS] = F_COMPTET_DEST.N_AnalytiqueIFRS\n" +
                "      ,[CA_NumIFRS] = F_COMPTET_DEST.CA_NumIFRS\n" +
                "      ,[CT_PrioriteLivr] = F_COMPTET_DEST.CT_PrioriteLivr\n" +
                "      ,[CT_LivrPartielle] = F_COMPTET_DEST.CT_LivrPartielle\n" +
                "      ,[MR_No] = F_COMPTET_DEST.MR_No\n" +
                "      ,[CT_NotPenal] = F_COMPTET_DEST.CT_NotPenal\n" +
                "      ,[EB_No] = F_COMPTET_DEST.EB_No\n" +
                "      ,[CT_NumCentrale] = F_COMPTET_DEST.CT_NumCentrale\n" +
                "      ,[CT_DateFermeDebut] = F_COMPTET_DEST.CT_DateFermeDebut\n" +
                "      ,[CT_DateFermeFin] = F_COMPTET_DEST.CT_DateFermeFin\n" +
                "      ,[CT_FactureElec] = F_COMPTET_DEST.CT_FactureElec\n" +
                "      ,[CT_TypeNIF] = F_COMPTET_DEST.CT_TypeNIF\n" +
                "      ,[CT_RepresentInt] = F_COMPTET_DEST.CT_RepresentInt\n" +
                "      ,[CT_RepresentNIF] = F_COMPTET_DEST.CT_RepresentNIF\n" +
                "      ,[cbProt] = F_COMPTET_DEST.cbProt\n" +
                "      ,[cbCreateur] = F_COMPTET_DEST.cbCreateur\n" +
                "      ,[cbModification] = F_COMPTET_DEST.cbModification\n" +
                "      ,[cbReplication] = F_COMPTET_DEST.cbReplication\n" +
                "      ,[cbFlag] = F_COMPTET_DEST.cbFlag\n" +
                "FROM F_COMPTET_DEST\n" +
                "WHERE\tF_COMPTET.CT_Num = F_COMPTET_DEST.CT_Num\n" +
                "            \n" +
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
                "   'F_COMPTET',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        /*readOnFile(path,file,"F_COMPTET_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"F_COMPTET_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());

         */
        deleteTempTable(sqlCon);
        deleteComptet(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        /*initTable(sqlCon);
        getData(sqlCon, list(), "F_COMPTET", path, file);
        listDeleteComptet(sqlCon, path, "deleteList" + file);*/
    }
    public static void initTable(Connection sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_COMPTET') " +
                " INSERT INTO config.ListComptet " +
                " SELECT CT_Num,CT_Type,cbMarq " +
                " FROM F_COMPTET ";
        executeQuery(sqlCon, query);
    }
    public static void deleteTempTable(Connection sqlCon)
    {
        String query = "IF OBJECT_ID('F_COMPTET_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_COMPTET_DEST;";
        executeQuery(sqlCon, query);
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
    public static void listDeleteComptet(Connection sqlCon, String path, String file)
    {
        String query = " SELECT lart.CT_Num,lart.CT_Type,lart.cbMarq " +
                " FROM config.ListComptet lart " +
                " LEFT JOIN dbo.F_COMPTET fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\" + file, query, sqlCon);

        query = " DELETE FROM config.ListComptet " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_COMPTET " +
                "                  WHERE dbo.F_COMPTET.cbMarq = config.ListComptet.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
