import org.json.simple.JSONObject;

import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;
import java.sql.SQLException;

public class DocEntete extends Table {
    public static String file = "docEntete_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_DOCENTETE";
    public static String configList = "listDocEntete";

    public static String insert(String filename)
    {
        return "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                "IF OBJECT_ID('tempdb..#DocEntete') IS NOT NULL\n" +
                "    DROP TABLE #DocEntete\n"+
                "SELECT dest.[DO_Domaine], dest.[DO_Type], dest.[DO_Date],[DO_Ref] = LEFT(dest.[DO_Ref],17) \n" +
                ", dest.[DO_Tiers], dest.[CO_No], dest.[DO_Period], dest.[DO_Devise] \n" +
                ", dest.[DO_Cours]\n" +
                ", DE_No = ISNULL(dep.DE_No,dest.[DE_No])\n" +
                ", LI_No = ISNULL(liv.[LI_No],dest.[LI_No]) \n" +
                ", dest.[CT_NumPayeur], dest.[DO_Expedit], dest.[DO_NbFacture], dest.[DO_BLFact] \n" +
                ", dest.[DO_TxEscompte], dest.[DO_Reliquat], dest.[DO_Imprim], dest.[CA_Num] \n" +
                ", dest.[DO_Coord01], dest.[DO_Coord02], dest.[DO_Coord03], dest.[DO_Coord04] \n" +
                ", dest.[DO_Souche], dest.[DO_DateLivr], dest.[DO_Condition], dest.[DO_Tarif] \n" +
                ", dest.[DO_Colisage], dest.[DO_TypeColis], dest.[DO_Transaction], dest.[DO_Langue] \n" +
                ", dest.[DO_Ecart], dest.[DO_Regime], dest.[N_CatCompta], dest.[DO_Ventile] \n" +
                ", dest.[AB_No], dest.[DO_DebutAbo], dest.[DO_FinAbo], dest.[DO_DebutPeriod] \n" +
                ", dest.[DO_FinPeriod], dest.[CG_Num], dest.[DO_Statut], dest.[DO_Heure] \n" +
                ", CA_No = ISNULL(cai.CA_No,dest.[CA_No]), dest.[CO_NoCaissier] \n" +
                ", dest.[DO_Transfere], dest.[DO_Cloture], dest.[DO_NoWeb], dest.[DO_Attente] \n" +
                ", dest.[DO_Provenance], dest.[CA_NumIFRS], dest.[MR_No], dest.[DO_TypeFrais] \n" +
                ", dest.[DO_ValFrais], dest.[DO_TypeLigneFrais], dest.[DO_TypeFranco], dest.[DO_ValFranco] \n" +
                ", dest.[DO_TypeLigneFranco], dest.[DO_Taxe1], dest.[DO_TypeTaux1], dest.[DO_TypeTaxe1] \n" +
                ", dest.[DO_Taxe2], dest.[DO_TypeTaux2], dest.[DO_TypeTaxe2], dest.[DO_Taxe3] \n" +
                ", dest.[DO_TypeTaux3], dest.[DO_TypeTaxe3], dest.[DO_MajCpta], dest.[DO_Motif] \n" +
                ", dest.[CT_NumCentrale], dest.[DO_Contact], dest.[DO_FactureElec], dest.[DO_TypeTransac] \n" +
                ", dest.[cbProt], dest.[cbCreateur], dest.[cbModification], dest.[cbReplication] \n" +
                ", dest.[cbFlag],dest.[DO_Piece],dest.[dataBaseSource],dest.[cbMarqSource],DO_DomaineSrc = src.DO_Domaine \n" +
                "INTO #DocEntete\n" +
                "FROM F_DOCENTETE_DEST dest \n" +
                "LEFT JOIN F_DOCENTETE src \n" +
                "ON dest.DO_Piece = src.DO_Piece \n" +
                "AND dest.DO_Domaine = src.DO_Domaine \n" +
                "AND dest.DO_Type = src.DO_Type \n" +
                "LEFT JOIN F_DEPOT dep ON dep.DE_NoSource = dest.DE_No AND dep.dataBaseSource = dest.dataBaseSource \n" +
                "LEFT JOIN F_CAISSE cai ON cai.CA_NoSource = dest.CA_No AND cai.dataBaseSource = dest.dataBaseSource \n" +
                "LEFT JOIN F_LIVRAISON liv ON liv.LI_NoSource = dest.LI_No AND liv.dataBaseSource = dest.dataBaseSource \n" +
                "" +
                " IF OBJECT_ID('F_DOCENTETE_DEST') IS NOT NULL\n"+
                "UPDATE dwh\n" +
                "   SET [DO_Domaine] = tmp.DO_Domaine\n" +
                "      ,[DO_Type] = tmp.DO_Type\n" +
                "      ,[DO_Piece] = tmp.DO_Piece\n" +
                "      ,[DO_Date] = tmp.DO_Date\n" +
                "      ,[DO_Ref] = tmp.DO_Ref\n" +
                "      ,[DO_Tiers] = tmp.DO_Tiers\n" +
                "      ,[CO_No] = tmp.CO_No\n" +
                "      ,[DO_Period] = tmp.DO_Period\n" +
                "      ,[DO_Devise] = tmp.DO_Devise\n" +
                "      ,[DO_Cours] = tmp.DO_Cours\n" +
                "      ,[DE_No] = tmp.DE_No\n" +
                "      ,[LI_No] = tmp.LI_No\n" +
                "      ,[CT_NumPayeur] = tmp.CT_NumPayeur\n" +
                "      ,[DO_Expedit] = tmp.DO_Expedit\n" +
                "      ,[DO_NbFacture] = tmp.DO_NbFacture\n" +
                "      ,[DO_BLFact] = tmp.DO_BLFact\n" +
                "      ,[DO_TxEscompte] = tmp.DO_TxEscompte\n" +
                "      ,[DO_Reliquat] = tmp.DO_Reliquat\n" +
                "      ,[DO_Imprim] = tmp.DO_Imprim\n" +
                "      ,[CA_Num] = tmp.CA_Num\n" +
                "      ,[DO_Coord01] = tmp.DO_Coord01\n" +
                "      ,[DO_Coord02] = tmp.DO_Coord02\n" +
                "      ,[DO_Coord03] = tmp.DO_Coord03\n" +
                "      ,[DO_Coord04] = tmp.DO_Coord04\n" +
                "      ,[DO_Souche] = tmp.DO_Souche\n" +
                "      ,[DO_DateLivr] = tmp.DO_DateLivr\n" +
                "      ,[DO_Condition] = tmp.DO_Condition\n" +
                "      ,[DO_Tarif] = tmp.DO_Tarif\n" +
                "      ,[DO_Colisage] = tmp.DO_Colisage\n" +
                "      ,[DO_TypeColis] = tmp.DO_TypeColis\n" +
                "      ,[DO_Transaction] = tmp.DO_Transaction\n" +
                "      ,[DO_Langue] = tmp.DO_Langue\n" +
                "      ,[DO_Ecart] = tmp.DO_Ecart\n" +
                "      ,[DO_Regime] = tmp.DO_Regime\n" +
                "      ,[N_CatCompta] = tmp.N_CatCompta\n" +
                "      ,[DO_Ventile] = tmp.DO_Ventile\n" +
                "      ,[AB_No] = tmp.AB_No\n" +
                "      ,[DO_DebutAbo] = tmp.DO_DebutAbo\n" +
                "      ,[DO_FinAbo] = tmp.DO_FinAbo\n" +
                "      ,[DO_DebutPeriod] = tmp.DO_DebutPeriod\n" +
                "      ,[DO_FinPeriod] = tmp.DO_FinPeriod\n" +
                "      ,[CG_Num] = tmp.CG_Num\n" +
                "      ,[DO_Statut] = tmp.DO_Statut\n" +
                "      ,[DO_Heure] = tmp.DO_Heure\n" +
                "      ,[CA_No] = tmp.CA_No\n" +
                "      ,[CO_NoCaissier] = tmp.CO_NoCaissier\n" +
                "      ,[DO_Transfere] = tmp.DO_Transfere\n" +
                "      ,[DO_Cloture] = tmp.DO_Cloture\n" +
                "      ,[DO_NoWeb] = tmp.DO_NoWeb\n" +
                "      ,[DO_Attente] = tmp.DO_Attente\n" +
                "      ,[DO_Provenance] = tmp.DO_Provenance\n" +
                "      ,[CA_NumIFRS] = tmp.CA_NumIFRS\n" +
                "      ,[MR_No] = tmp.MR_No\n" +
                "      ,[DO_TypeFrais] = tmp.DO_TypeFrais\n" +
                "      ,[DO_ValFrais] = tmp.DO_ValFrais\n" +
                "      ,[DO_TypeLigneFrais] = tmp.DO_TypeLigneFrais\n" +
                "      ,[DO_TypeFranco] = tmp.DO_TypeFranco\n" +
                "      ,[DO_ValFranco] = tmp.DO_ValFranco\n" +
                "      ,[DO_TypeLigneFranco] = tmp.DO_TypeLigneFranco\n" +
                "      ,[DO_Taxe1] = tmp.DO_Taxe1\n" +
                "      ,[DO_TypeTaux1] = tmp.DO_TypeTaux1\n" +
                "      ,[DO_TypeTaxe1] = tmp.DO_TypeTaxe1\n" +
                "      ,[DO_Taxe2] = tmp.DO_Taxe2\n" +
                "      ,[DO_TypeTaux2] = tmp.DO_TypeTaux2\n" +
                "      ,[DO_TypeTaxe2] = tmp.DO_TypeTaxe2\n" +
                "      ,[DO_Taxe3] = tmp.DO_Taxe3\n" +
                "      ,[DO_TypeTaux3] = tmp.DO_TypeTaux3\n" +
                "      ,[DO_TypeTaxe3] = tmp.DO_TypeTaxe3\n" +
                "      ,[DO_MajCpta] = tmp.DO_MajCpta\n" +
                "      ,[DO_Motif] = tmp.DO_Motif\n" +
                "      ,[CT_NumCentrale] = tmp.CT_NumCentrale\n" +
                "      ,[DO_Contact] = tmp.DO_Contact\n" +
                "      ,[DO_FactureElec] = tmp.DO_FactureElec\n" +
                "      ,[DO_TypeTransac] = tmp.DO_TypeTransac\n" +
                "      ,[cbProt] = tmp.cbProt\n" +
                "      ,[cbCreateur] = tmp.cbCreateur\n" +
                "      ,[cbModification] = tmp.cbModification\n" +
                "      ,[cbReplication] = tmp.cbReplication\n" +
                "      ,[cbFlag] = tmp.cbFlag\n" +
                "\t  \n" +
                "FROM [dbo].[F_DOCENTETE] dwh\n" +
                "INNER JOIN #DocEntete tmp ON dwh.[DataBaseSource] = tmp.[DataBaseSource]\n" +
                "AND dwh.cbMarqSource = tmp.cbMarqSource" +
                ""+
                " IF OBJECT_ID('F_DOCENTETE_DEST') IS NOT NULL\n"+
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
                "\t\t\t, [cbFlag],[DO_Piece],[DataBaseSource],[cbMarqSource])\n" +
                "            \n" +
                "SELECT [DO_Domaine], [DO_Type], [DO_Date], [DO_Ref]\n" +
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
                "\t\t\t, [cbFlag],[DO_Piece],[dataBaseSource],[cbMarqSource]\n" +
                "FROM #DocEntete " +
                "WHERE DO_DomaineSrc IS NULL;\n" +
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
                "   'Insert '+ ' "+filename+"',\n" +
                "   'F_DOCENTETE',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {
        dbSource = database;
        loadFile(path,sqlCon);
        //loadDeleteFile(path,sqlCon);
    }

    public static void loadFile(String path,Connection sqlCon){
        String [] children = getFile(path,file);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                disableTrigger(sqlCon,tableName);
//                executeQuery(sqlCon, updateTableDest("", "'DO_Domaine','DO_Type','DO_Piece','DataBaseSource','cbMarqSource'", tableName, tableName + "_DEST",filename));
                sendData(sqlCon, path, filename, insert(filename));
                //deleteTempTable(sqlCon, tableName+"_DEST");
                enableTrigger(sqlCon,tableName);
            }
        }
    }

    public static void loadDeleteFile(String path,Connection sqlCon) {
        String [] children = getFile(path,"deleteList"+file);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                String deleteDocEntete = deleteDocEntete(filename);
                loadDeleteInfo(path,tableName,filename,sqlCon,deleteDocEntete);
            }
        }
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time,JSONObject type)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,type), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

    public static void getDataElementFilterAgency(Connection sqlCon, String path,String database,String time,String agency)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DatabaseSource");
        getData(sqlCon, selectSourceTableFilterAgency(tableName,database,agency,"DE_No"), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

    public static String deleteDocEntete(String filename)
    {
        return
                "BEGIN TRY " +
                /*" DELETE FROM F_DOCLIGNE  " +
                "   WHERE EXISTS (SELECT 1 "+
                "       FROM F_DOCENTETE_SUPPR suppr\n" +
                "       WHERE F_DOCLIGNE.DataBaseSource = suppr.DataBaseSource \n" +
                "       AND F_DOCLIGNE.DO_Domaine = suppr.DO_Domaine \n" +
                "       AND F_DOCLIGNE.DO_Type = suppr.DO_Type \n" +
                "       AND F_DOCLIGNE.DO_Piece = suppr.DO_Piece ); \n" +
                " DELETE FROM F_DOCREGL  " +
                "   WHERE EXISTS (SELECT 1 "+
                "       FROM F_DOCENTETE_SUPPR suppr\n" +
                "       WHERE F_DOCREGL.DataBaseSource = suppr.DataBaseSource \n" +
                "       AND F_DOCREGL.DO_Domaine = suppr.DO_Domaine \n" +
                "       AND F_DOCREGL.DO_Type = suppr.DO_Type \n" +
                "       AND F_DOCREGL.DO_Piece = suppr.DO_Piece ); \n" +
                " DELETE FROM F_REGLECH  " +
                "   WHERE EXISTS (SELECT 1 "+
                "       FROM F_DOCENTETE_SUPPR suppr\n" +
                "       WHERE F_REGLECH.DataBaseSource = suppr.DataBaseSource \n" +
                "       AND F_REGLECH.DO_Domaine = suppr.DO_Domaine \n" +
                "       AND F_REGLECH.DO_Type = suppr.DO_Type \n" +
                "       AND F_REGLECH.DO_Piece = suppr.DO_Piece ); \n" +*/
                "\n" +
                "DISABLE TRIGGER dbo.[TG_CBDEL_F_DOCENTETE] ON [dbo].[F_DOCENTETE] ;\n" +
                "DISABLE TRIGGER dbo.[TG_DEL_F_DOCENTETE] ON [dbo].[F_DOCENTETE] ;\n" +
                " DELETE FROM F_DOCENTETE \n" +
                " WHERE EXISTS (SELECT 1 \n" +
                "       FROM F_DOCENTETE_SUPPR \n" +
                "       WHERE F_DOCENTETE.DataBaseSource = F_DOCENTETE_SUPPR.DataBaseSource \n" +
                "       AND F_DOCENTETE.DO_Domaine = F_DOCENTETE_SUPPR.DO_Domaine \n" +
                "       AND F_DOCENTETE.DO_Type = F_DOCENTETE_SUPPR.DO_Type \n" +
                "       AND F_DOCENTETE.DO_Piece = F_DOCENTETE_SUPPR.DO_Piece ) ;\n" +
                /*" AND NOT EXISTS (SELECT 1 \n" +
                "       FROM F_DOCLIGNE docL \n" +
                "       WHERE F_DOCENTETE.DataBaseSource = docL.DataBaseSource \n" +
                "       AND F_DOCENTETE.DO_Domaine = docL.DO_Domaine \n" +
                "       AND F_DOCENTETE.DO_Type = docL.DO_Type \n" +
                "       AND F_DOCENTETE.DO_Piece = docL.DO_Piece ) \n" +
                " AND NOT EXISTS (SELECT 1 \n" +
                "       FROM F_DOCREGL docL \n" +
                "       WHERE F_DOCENTETE.DataBaseSource = docL.DataBaseSource \n" +
                "       AND F_DOCENTETE.DO_Domaine = docL.DO_Domaine \n" +
                "       AND F_DOCENTETE.DO_Type = docL.DO_Type \n" +
                "       AND F_DOCENTETE.DO_Piece = docL.DO_Piece ) \n" +
                " AND NOT EXISTS (SELECT 1 \n" +
                "       FROM F_REGLECH docL \n" +
                "       WHERE F_DOCENTETE.DataBaseSource = docL.DataBaseSource \n" +
                "       AND F_DOCENTETE.DO_Domaine = docL.DO_Domaine \n" +
                "       AND F_DOCENTETE.DO_Type = docL.DO_Type \n" +
                "       AND F_DOCENTETE.DO_Piece = docL.DO_Piece ) \n" +*/
                "ENABLE TRIGGER dbo.[TG_CBDEL_F_DOCENTETE] ON [dbo].[F_DOCENTETE] ;\n" +
                "ENABLE TRIGGER dbo.[TG_DEL_F_DOCENTETE] ON [dbo].[F_DOCENTETE] ;"+
                " \n" +
                " \n" +
                "INSERT INTO config.DB_Errors(\n" +
                "          UserName,\n" +
                "          ErrorNumber,\n" +
                "          ErrorState,\n" +
                "          ErrorSeverity,\n" +
                "          ErrorLine,\n" +
                "          ErrorProcedure,\n" +
                "          ErrorMessage,\n" +
                "          TableLoad,\n" +
                "          Query,\n" +
                "          ErrorDateTime)\n" +
                "   SELECT\t   SUSER_SNAME()," +
                "           NULL," +
                "           NULL," +
                "           NULL," +
                "           NULL," +
                "           NULL," +
                "           NULL,'Domaine : '+ CAST(docE.[DO_Domaine] AS NVARCHAR(50)) +' Type : ' + CAST(docE.[DO_Type] AS NVARCHAR(50)) + ' Piece : ' + docE.[DO_Piece]" +
                "           +' cbMarq : ' + CAST(docE.[cbMarqSource]  AS NVARCHAR(50)) + ' database : ' + docE.[DataBaseSource]" +
                "           + ' fileName : "+filename+" '" +
                "           ,'F_DOCLIGNE'\n" +
                "           ,GETDATE()\n" +
                " FROM F_DOCENTETE docE\n" +
                " WHERE EXISTS (SELECT 1 \n" +
                "       FROM F_DOCENTETE_SUPPR docL \n" +
                "       WHERE docE.DataBaseSource = docL.DataBaseSource \n" +
                "       AND docE.DO_Domaine = docL.DO_Domaine \n" +
                "       AND docE.DO_Type = docL.DO_Type \n" +
                "       AND docE.DO_Piece = docL.DO_Piece ) \n" +
                " AND NOT EXISTS (SELECT 1 \n" +
                "       FROM F_DOCLIGNE docL \n" +
                "       WHERE docE.DataBaseSource = docL.DataBaseSource \n" +
                "       AND docE.DO_Domaine = docL.DO_Domaine \n" +
                "       AND docE.DO_Type = docL.DO_Type \n" +
                "       AND docE.DO_Piece = docL.DO_Piece ) \n" +
                " AND NOT EXISTS (SELECT 1 \n" +
                "       FROM F_DOCREGL docL \n" +
                "       WHERE docE.DataBaseSource = docL.DataBaseSource \n" +
                "       AND docE.DO_Domaine = docL.DO_Domaine \n" +
                "       AND docE.DO_Type = docL.DO_Type \n" +
                "       AND docE.DO_Piece = docL.DO_Piece ) \n" +
                " AND NOT EXISTS (SELECT 1 \n" +
                "       FROM F_REGLECH docL \n" +
                "       WHERE docE.DataBaseSource = docL.DataBaseSource \n" +
                "       AND docE.DO_Domaine = docL.DO_Domaine \n" +
                "       AND docE.DO_Type = docL.DO_Type \n" +
                "       AND docE.DO_Piece = docL.DO_Piece ) \n" +
                " \n" +
                /*" IF OBJECT_ID('F_DOCENTETE_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_DOCENTETE_SUPPR \n"+*/
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
                        "   'Delete '+ ' "+filename+"',\n" +
                        "   'F_DOCENTETE',\n" +
                        "   GETDATE());\n" +
                        "END CATCH";

    }
}
