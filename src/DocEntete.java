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

    public static String insert()
    {
        return "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
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
                "SELECT dest.[DO_Domaine], dest.[DO_Type], dest.[DO_Date], LEFT(dest.[DO_Ref],17)\n" +
                "\t, dest.[DO_Tiers], dest.[CO_No], dest.[DO_Period], dest.[DO_Devise]\n" +
                "\t, dest.[DO_Cours], ISNULL(dep.DE_No,dest.[DE_No]), dest.[LI_No]\n" +
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
                "\t\t\t, dest.[cbFlag],dest.[DO_Piece],dest.[dataBaseSource],dest.[cbMarqSource]\n" +
                "FROM F_DOCENTETE_DEST dest\n" +
                "LEFT JOIN F_DOCENTETE src\n" +
                "\tON\tdest.cbMarqSource = src.cbMarqSource\n" +
                "\tAND\tdest.dataBaseSource = src.dataBaseSource\n" +
                "LEFT JOIN F_DEPOT dep ON dep.DE_NoSource = src.DE_No AND dep.dataBaseSource = src.dataBaseSource \n"+
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
                "   'Insert',\n" +
                "   'F_DOCENTETE',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {
        File dir = new File(path);
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith(file);
            }
        };
        String[] children = dir.list(filter);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (int i = 0; i < children.length; i++) {
                String filename = children[i];
                dbSource = database;
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                readOnFile(path, "deleteList" + filename, tableName + "_SUPPR", sqlCon);
                executeQuery(sqlCon, updateTableDest("", "'DO_Domaine','DO_Type','DO_Piece','DataBaseSource','cbMarqSource'", tableName, tableName + "_DEST"));
                sendData(sqlCon, path, filename, insert());

                deleteTempTable(sqlCon, tableName);
                deleteDocEntete(sqlCon, path,filename);
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

    public static void deleteDocEntete(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_DOCENTETE \n" +
                " WHERE EXISTS (SELECT 1 FROM F_DOCENTETE_SUPPR WHERE F_DOCENTETE.DataBaseSource = F_DOCENTETE_SUPPR.DataBaseSource AND F_DOCENTETE.DO_Domaine = F_DOCENTETE_SUPPR.DO_Domaine AND F_DOCENTETE.DO_Type = F_DOCENTETE_SUPPR.DO_Type AND F_DOCENTETE.DO_Piece = F_DOCENTETE_SUPPR.DO_Piece ) \n" +
                " \n" +
                " IF OBJECT_ID('F_DOCENTETE_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_DOCENTETE_SUPPR \n";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }
}
