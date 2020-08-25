import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

public class DocEntete extends Table {
    public static String file = "docEntete.csv";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_DOCENTETE";
    public static String configList = "listDocEntete";

    public static String list()
    {
        return "SELECT \n" +
                "\t[DO_Domaine], [DO_Type], [DO_Date], [DO_Ref]\n" +
                "\t, [DO_Tiers], [CO_No], [DO_Period], [DO_Devise]\n" +
                "\t, [DO_Cours], [DE_No], [LI_No]\n" +
                "\t, [CT_NumPayeur], [DO_Expedit], [DO_NbFacture], [DO_BLFact]\n" +
                "\t, [DO_TxEscompte], [DO_Reliquat], [DO_Imprim], [CA_Num]\n" +
                "\t, [DO_Coord01], [DO_Coord02], [DO_Coord03], [DO_Coord04]\n" +
                "\t, [DO_Souche], [DO_DateLivr], [DO_Condition], [DO_Tarif]\n" +
                "\t, [DO_Colisage], [DO_TypeColis], [DO_Transaction], [DO_Langue]\n" +
                "\t, [DO_Ecart], [DO_Regime], [N_CatCompta], [DO_Ventile]\n" +
                "\t, [AB_No], [DO_DebutAbo], [DO_FinAbo], [DO_DebutPeriod]\n" +
                "\t, [DO_FinPeriod], [CG_Num], [DO_Statut], [DO_Heure]\n" +
                "\t, [CA_No], [CO_NoCaissier]\n" +
                "\t, [DO_Transfere], [DO_Cloture], [DO_NoWeb], [DO_Attente]\n" +
                "\t, [DO_Provenance], [CA_NumIFRS], [MR_No], [DO_TypeFrais]\n" +
                "\t, [DO_ValFrais], [DO_TypeLigneFrais], [DO_TypeFranco], [DO_ValFranco]\n" +
                "\t\t, [DO_TypeLigneFranco], [DO_Taxe1], [DO_TypeTaux1], [DO_TypeTaxe1]\n" +
                "\t\t, [DO_Taxe2], [DO_TypeTaux2], [DO_TypeTaxe2], [DO_Taxe3]\n" +
                "\t\t, [DO_TypeTaux3], [DO_TypeTaxe3], [DO_MajCpta], [DO_Motif]\n" +
                "\t\t\t, [CT_NumCentrale], [DO_Contact], [DO_FactureElec], [DO_TypeTransac]\n" +
                "\t\t\t, [cbProt], [cbCreateur], [cbModification], [cbReplication]\n" +
                "\t\t\t, [cbFlag],[DO_Piece], cbMarqSource = cbMarq\n" +
                "\t\t,[DataBaseSource] = '" + dbSource + "' \n" +
                "FROM [dbo].[F_DOCENTETE] " +
                "WHERE cbModification>= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName = 'F_DOCENTETE'),'1900-01-01')";
    }

    public static String insert()
    {
        return "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                "UPDATE F_DOCENTETE \n" +
                "SET [DO_Domaine] = F_DOCENTETE_DEST.DO_Domaine,[DO_Type] = F_DOCENTETE_DEST.DO_Type\n" +
                "      ,[DO_Piece] = F_DOCENTETE_DEST.DO_Piece,[DO_Date] = F_DOCENTETE_DEST.DO_Date\n" +
                "      ,[DO_Ref] = LEFT(F_DOCENTETE_DEST.DO_Ref,17),[DO_Tiers] = F_DOCENTETE_DEST.DO_Tiers\n" +
                "      ,[CO_No] = F_DOCENTETE_DEST.CO_No,[DO_Period] = F_DOCENTETE_DEST.DO_Period\n" +
                "      ,[DO_Devise] = F_DOCENTETE_DEST.DO_Devise,[DO_Cours] = F_DOCENTETE_DEST.DO_Cours\n" +
                "      ,[DE_No] = F_DOCENTETE_DEST.DE_No,[LI_No] = F_DOCENTETE_DEST.LI_No\n" +
                "      ,[CT_NumPayeur] = F_DOCENTETE_DEST.CT_NumPayeur,[DO_Expedit] = F_DOCENTETE_DEST.DO_Expedit\n" +
                "      ,[DO_NbFacture] = F_DOCENTETE_DEST.DO_NbFacture,[DO_BLFact] = F_DOCENTETE_DEST.DO_BLFact\n" +
                "      ,[DO_TxEscompte] = F_DOCENTETE_DEST.DO_TxEscompte,[DO_Reliquat] = F_DOCENTETE_DEST.DO_Reliquat\n" +
                "      ,[DO_Imprim] = F_DOCENTETE_DEST.DO_Imprim,[CA_Num] = F_DOCENTETE_DEST.CA_Num\n" +
                "      ,[DO_Coord01] = F_DOCENTETE_DEST.DO_Coord01,[DO_Coord02] = F_DOCENTETE_DEST.DO_Coord02\n" +
                "      ,[DO_Coord03] = F_DOCENTETE_DEST.DO_Coord03,[DO_Coord04] = F_DOCENTETE_DEST.DO_Coord04\n" +
                "      ,[DO_Souche] = F_DOCENTETE_DEST.DO_Souche,[DO_DateLivr] = F_DOCENTETE_DEST.DO_DateLivr\n" +
                "      ,[DO_Condition] = F_DOCENTETE_DEST.DO_Condition,[DO_Tarif] = F_DOCENTETE_DEST.DO_Tarif\n" +
                "      ,[DO_Colisage] = F_DOCENTETE_DEST.DO_Colisage,[DO_TypeColis] = F_DOCENTETE_DEST.DO_TypeColis\n" +
                "      ,[DO_Transaction] = F_DOCENTETE_DEST.DO_Transaction,[DO_Langue] = F_DOCENTETE_DEST.DO_Langue\n" +
                "      ,[DO_Ecart] = F_DOCENTETE_DEST.DO_Ecart,[DO_Regime] = F_DOCENTETE_DEST.DO_Regime\n" +
                "      ,[N_CatCompta] = F_DOCENTETE_DEST.N_CatCompta,[DO_Ventile] = F_DOCENTETE_DEST.DO_Ventile\n" +
                "      ,[AB_No] = F_DOCENTETE_DEST.AB_No,[DO_DebutAbo] = F_DOCENTETE_DEST.DO_DebutAbo\n" +
                "      ,[DO_FinAbo] = F_DOCENTETE_DEST.DO_FinAbo,[DO_DebutPeriod] = F_DOCENTETE_DEST.DO_DebutPeriod\n" +
                "      ,[DO_FinPeriod] = F_DOCENTETE_DEST.DO_FinPeriod,[CG_Num] = F_DOCENTETE_DEST.CG_Num\n" +
                "      ,[DO_Statut] = F_DOCENTETE_DEST.DO_Statut,[DO_Heure] = F_DOCENTETE_DEST.DO_Heure\n" +
                "      ,[CA_No] = F_DOCENTETE_DEST.CA_No,[CO_NoCaissier] = F_DOCENTETE_DEST.CO_NoCaissier\n" +
                "      ,[DO_Transfere] = F_DOCENTETE_DEST.DO_Transfere,[DO_Cloture] = F_DOCENTETE_DEST.DO_Cloture\n" +
                "      ,[DO_NoWeb] = F_DOCENTETE_DEST.DO_NoWeb,[DO_Attente] = F_DOCENTETE_DEST.DO_Attente\n" +
                "      ,[DO_Provenance] = F_DOCENTETE_DEST.DO_Provenance,[CA_NumIFRS] = F_DOCENTETE_DEST.CA_NumIFRS\n" +
                "      ,[MR_No] = F_DOCENTETE_DEST.MR_No,[DO_TypeFrais] = F_DOCENTETE_DEST.DO_TypeFrais\n" +
                "      ,[DO_ValFrais] = F_DOCENTETE_DEST.DO_ValFrais,[DO_TypeLigneFrais] = F_DOCENTETE_DEST.DO_TypeLigneFrais\n" +
                "      ,[DO_TypeFranco] = F_DOCENTETE_DEST.DO_TypeFranco,[DO_ValFranco] = F_DOCENTETE_DEST.DO_ValFranco\n" +
                "      ,[DO_TypeLigneFranco] = F_DOCENTETE_DEST.DO_TypeLigneFranco,[DO_Taxe1] = F_DOCENTETE_DEST.DO_Taxe1\n" +
                "      ,[DO_TypeTaux1] = F_DOCENTETE_DEST.DO_TypeTaux1,[DO_TypeTaxe1] = F_DOCENTETE_DEST.DO_TypeTaxe1\n" +
                "      ,[DO_Taxe2] = F_DOCENTETE_DEST.DO_Taxe2,[DO_TypeTaux2] = F_DOCENTETE_DEST.DO_TypeTaux2\n" +
                "      ,[DO_TypeTaxe2] = F_DOCENTETE_DEST.DO_TypeTaxe2,[DO_Taxe3] = F_DOCENTETE_DEST.DO_Taxe3\n" +
                "      ,[DO_TypeTaux3] = F_DOCENTETE_DEST.DO_TypeTaux3,[DO_TypeTaxe3] = F_DOCENTETE_DEST.DO_TypeTaxe3\n" +
                "      ,[DO_MajCpta] = F_DOCENTETE_DEST.DO_MajCpta,[DO_Motif] = F_DOCENTETE_DEST.DO_Motif\n" +
                "      ,[CT_NumCentrale] = F_DOCENTETE_DEST.CT_NumCentrale,[DO_Contact] = F_DOCENTETE_DEST.DO_Contact\n" +
                "      ,[DO_FactureElec] = F_DOCENTETE_DEST.DO_FactureElec,[DO_TypeTransac] = F_DOCENTETE_DEST.DO_TypeTransac\n" +
                "      ,[cbProt] = F_DOCENTETE_DEST.cbProt,[cbCreateur] = F_DOCENTETE_DEST.cbCreateur\n" +
                "      ,[cbModification] = F_DOCENTETE_DEST.cbModification,[cbReplication] = F_DOCENTETE_DEST.cbReplication\n" +
                "      ,[cbFlag] = F_DOCENTETE_DEST.cbFlag\n" +
                "FROM F_DOCENTETE_DEST\n" +
                "WHERE\tF_DOCENTETE.DO_Domaine = F_DOCENTETE_DEST.DO_Domaine\n" +
                "\tAND\tF_DOCENTETE.DO_Type = F_DOCENTETE_DEST.DO_Type\n" +
                "\tAND\tF_DOCENTETE.DO_Piece = F_DOCENTETE_DEST.DO_Piece\n" +
                "\tAND\tF_DOCENTETE.DataBaseSource = F_DOCENTETE_DEST.DataBaseSource;\n" +
                " INSERT INTO F_DOCENTETE (\n" +
                "[DO_Domaine], [DO_Type], [DO_Date], [DO_Ref]\n" +
                "\t, [DO_Tiers], [CO_No], [DO_Period], [DO_Devise]\n" +
                "\t, [DO_Cours], [DE_No], [LI_No]\n" +
                "\t, [CT_NumPayeur], [DO_Expedit], [DO_NbFacture], [DO_BLFact]\n" +
                "\t, [DO_TxEscompte], [DO_Reliquat], [DO_Imprim], [CA_Num]\n" +
                "\t, [DO_Coord01], [DO_Coord02], [DO_Coord03], [DO_Coord04]\n" +
                "\t, [DO_Souche], [DO_DateLivr], [DO_Condition], [DO_Tarif]\n" +
                "\t, [DO_Colisage], [DO_TypeColis], [DO_Transaction], [DO_Langue]\n" +
                "\t, [DO_Ecart], [DO_Regime], [N_CatCompta], [DO_Ventile]\n" +
                "\t, [AB_No], [DO_DebutAbo], [DO_FinAbo], [DO_DebutPeriod]\n" +
                "\t, [DO_FinPeriod], [CG_Num], [DO_Statut], [DO_Heure]\n" +
                "\t, [CA_No], [CO_NoCaissier]\n" +
                "\t, [DO_Transfere], [DO_Cloture], [DO_NoWeb], [DO_Attente]\n" +
                "\t, [DO_Provenance], [CA_NumIFRS], [MR_No], [DO_TypeFrais]\n" +
                "\t, [DO_ValFrais], [DO_TypeLigneFrais], [DO_TypeFranco], [DO_ValFranco]\n" +
                "\t\t, [DO_TypeLigneFranco], [DO_Taxe1], [DO_TypeTaux1], [DO_TypeTaxe1]\n" +
                "\t\t, [DO_Taxe2], [DO_TypeTaux2], [DO_TypeTaxe2], [DO_Taxe3]\n" +
                "\t\t, [DO_TypeTaux3], [DO_TypeTaxe3], [DO_MajCpta], [DO_Motif]\n" +
                "\t\t\t, [CT_NumCentrale], [DO_Contact], [DO_FactureElec], [DO_TypeTransac]\n" +
                "\t\t\t, [cbProt], [cbCreateur], [cbModification], [cbReplication]\n" +
                "\t\t\t, [cbFlag],[DO_Piece],DataBaseSource)\n" +
                "            \n" +
                "SELECT dest.[DO_Domaine], dest.[DO_Type], dest.[DO_Date], LEFT(dest.[DO_Ref],17)\n" +
                "\t, dest.[DO_Tiers], dest.[CO_No], dest.[DO_Period], dest.[DO_Devise]\n" +
                "\t, dest.[DO_Cours], dest.[DE_No], dest.[LI_No]\n" +
                "\t, dest.[CT_NumPayeur], dest.[DO_Expedit], dest.[DO_NbFacture], dest.[DO_BLFact]\n" +
                "\t, dest.[DO_TxEscompte], dest.[DO_Reliquat], dest.[DO_Imprim], dest.[CA_Num]\n" +
                "\t, dest.[DO_Coord01], dest.[DO_Coord02], dest.[DO_Coord03], dest.[DO_Coord04]\n" +
                "\t, dest.[DO_Souche], dest.[DO_DateLivr], dest.[DO_Condition], dest.[DO_Tarif]\n" +
                "\t, dest.[DO_Colisage], dest.[DO_TypeColis], dest.[DO_Transaction], dest.[DO_Langue]\n" +
                "\t, dest.[DO_Ecart], dest.[DO_Regime], dest.[N_CatCompta], dest.[DO_Ventile]\n" +
                "\t, dest.[AB_No], dest.[DO_DebutAbo], dest.[DO_FinAbo], dest.[DO_DebutPeriod]\n" +
                "\t, dest.[DO_FinPeriod], dest.[CG_Num], dest.[DO_Statut], dest.[DO_Heure]\n" +
                "\t, dest.[CA_No], dest.[CO_NoCaissier]\n" +
                "\t, dest.[DO_Transfere], dest.[DO_Cloture], dest.[DO_NoWeb], dest.[DO_Attente]\n" +
                "\t, dest.[DO_Provenance], dest.[CA_NumIFRS], dest.[MR_No], dest.[DO_TypeFrais]\n" +
                "\t, dest.[DO_ValFrais], dest.[DO_TypeLigneFrais], dest.[DO_TypeFranco], dest.[DO_ValFranco]\n" +
                "\t\t, dest.[DO_TypeLigneFranco], dest.[DO_Taxe1], dest.[DO_TypeTaux1], dest.[DO_TypeTaxe1]\n" +
                "\t\t, dest.[DO_Taxe2], dest.[DO_TypeTaux2], dest.[DO_TypeTaxe2], dest.[DO_Taxe3]\n" +
                "\t\t, dest.[DO_TypeTaux3], dest.[DO_TypeTaxe3], dest.[DO_MajCpta], dest.[DO_Motif]\n" +
                "\t\t\t, dest.[CT_NumCentrale], dest.[DO_Contact], dest.[DO_FactureElec], dest.[DO_TypeTransac]\n" +
                "\t\t\t, dest.[cbProt], dest.[cbCreateur], dest.[cbModification], dest.[cbReplication]\n" +
                "\t\t\t, dest.[cbFlag],dest.[DO_Piece],dest.[dataBaseSource]\n" +
                "FROM F_DOCENTETE_DEST dest\n" +
                "LEFT JOIN F_DOCENTETE src\n" +
                "\tON\tdest.DO_Domaine = src.DO_Domaine\n" +
                "\tAND\tdest.DO_Type = src.DO_Type\n" +
                "\tAND\tdest.DO_Piece = src.DO_Piece\n" +
                "\tAND\tdest.dataBaseSource = src.dataBaseSource\n" +
                "WHERE src.DO_Domaine IS NULL;\n" +
                "IF OBJECT_ID('F_DOCENTETE_DEST') IS NOT NULL \n" +
                "DROP TABLE F_DOCENTETE_DEST;\n" +
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
                "   'F_DOCENTETE',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {
        dbSource = database;

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        /*
        readOnFile(path,file,"F_DOCENTETE_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"F_DOCENTETE_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());

         */
        deleteTempTable(sqlCon);
        deleteDocEntete(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        /*initTable(sqlCon);
        getData(sqlCon, list(), "F_DOCENTETE", path, file);
        listDeleteDocEntete(sqlCon, path, "deleteList" + file);

         */
    }
    public static void initTable(Connection sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_DOCENTETE') " +
                " INSERT INTO config.ListDocEntete " +
                " SELECT DO_Domaine,DO_Type,DO_Piece,DataBaseSource = '" + dbSource + "',cbMarq " +
                " FROM F_DOCENTETE ";
        executeQuery(sqlCon, query);
    }
    public static void deleteTempTable(Connection sqlCon)
    {
        String query = "IF OBJECT_ID('F_DOCENTETE_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_DOCENTETE_DEST;";
        executeQuery(sqlCon, query);
    }
    public static void deleteDocEntete(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_DOCENTETE \n" +
                " WHERE EXISTS (SELECT 1 FROM F_DOCENTETE_SUPPR WHERE F_DOCENTETE.DataBaseSource = F_DOCENTETE_SUPPR.DataBaseSource AND F_DOCENTETE.DO_Domaine = F_DOCENTETE_SUPPR.DO_Domaine AND F_DOCENTETE.DO_Type = F_DOCENTETE_SUPPR.DO_Type AND F_DOCENTETE.DO_Piece = F_DOCENTETE_SUPPR.DO_Piece ) \n" +
                " \n" +
                " IF OBJECT_ID('F_DOCENTETE_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_DOCENTETE_SUPPR \n";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }
    public static void listDeleteDocEntete(Connection sqlCon, String path, String file)
    {
        String query = " SELECT lart.DO_Domaine,lart.DO_Type,lart.DO_Piece,lart.DataBaseSource,lart.cbMarq " +
                " FROM config.ListDocEntete lart " +
                " LEFT JOIN dbo.F_DOCENTETE fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\" + file, query, sqlCon);

        query = " DELETE FROM config.ListDocEntete " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_DOCENTETE " +
                "                  WHERE dbo.F_DOCENTETE.cbMarq = config.ListDocEntete.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
