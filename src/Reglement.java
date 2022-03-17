import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;
import java.sql.SQLException;

public class Reglement extends Table {

    public static String file = "Reglement_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_CREGLEMENT";
    public static String configList = "listReglement";
    public static String list()
    {
        return "SELECT\t[RG_No],[CT_NumPayeur],[RG_Date],[RG_Reference],[RG_Libelle],[RG_Montant],[RG_MontantDev],[N_Reglement],[RG_Impute]\n" +
                "\t\t,[RG_Compta],[EC_No],[RG_Type],[RG_Cours],[N_Devise],[JO_Num],[CG_NumCont],[RG_Impaye]\n" +
                "\t\t,[CG_Num],[RG_TypeReg],[RG_Heure],[RG_Piece],[CA_No],[CO_NoCaissier],[RG_Banque],[RG_Transfere]\n" +
                "\t\t,[RG_Cloture],[RG_Ticket],[RG_Souche],[CT_NumPayeurOrig],[RG_DateEchCont],[CG_NumEcart],[JO_NumEcart],[RG_MontantEcart]\n" +
                "\t\t,[RG_NoBonAchat],[cbProt],[cbMarq],[cbCreateur],[cbModification],[cbReplication],[cbFlag], cbMarqSource = cbMarq\n" +
                "\t\t,[DataBaseSource] = '"+ dbSource +"' \n" +
                "FROM\t[F_CREGLEMENT]\n" +
                "WHERE\tcbModification>= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName = 'F_CREGLEMENT'),'1900-01-01')";
    }

    public static String insert()
    {
        return "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                "UPDATE\tF_CREGLEMENT_DEST\n" +
                "SET\t[RG_Montant] = REPLACE([RG_Montant],',','.')\n" +
                "\t,[RG_MontantDev] = REPLACE([RG_MontantDev],',','.')\n" +
                "\t,[RG_MontantEcart] = REPLACE([RG_MontantEcart],',','.')\n" +
                "\t, [RG_Cours] = REPLACE([RG_Cours],',','.')\n" +
                "\n" +
                "UPDATE [dbo].[F_CREGLEMENT]\n" +
                "   SET [CT_NumPayeur] = F_CREGLEMENT_DEST.CT_NumPayeur\n" +
                "      ,[RG_Date] = F_CREGLEMENT_DEST.RG_Date\n" +
                "      ,[RG_Reference] = F_CREGLEMENT_DEST.RG_Reference\n" +
                "      ,[RG_Libelle] = F_CREGLEMENT_DEST.RG_Libelle\n" +
                "      ,[RG_Montant] = F_CREGLEMENT_DEST.RG_Montant\n" +
                "      ,[RG_MontantDev] = F_CREGLEMENT_DEST.RG_MontantDev\n" +
                "      ,[N_Reglement] = F_CREGLEMENT_DEST.N_Reglement\n" +
                "      ,[RG_Impute] = F_CREGLEMENT_DEST.RG_Impute\n" +
                "      ,[RG_Compta] = F_CREGLEMENT_DEST.RG_Compta\n" +
                "      ,[EC_No] = F_CREGLEMENT_DEST.EC_No\n" +
                "      ,[RG_Type] = F_CREGLEMENT_DEST.RG_Type\n" +
                "      ,[RG_Cours] = F_CREGLEMENT_DEST.RG_Cours\n" +
                "      ,[N_Devise] = F_CREGLEMENT_DEST.N_Devise\n" +
                "      ,[JO_Num] = F_CREGLEMENT_DEST.JO_Num\n" +
                "      ,[CG_NumCont] = F_CREGLEMENT_DEST.CG_NumCont\n" +
                "      ,[RG_Impaye] = F_CREGLEMENT_DEST.RG_Impaye\n" +
                "      ,[CG_Num] = F_CREGLEMENT_DEST.CG_Num\n" +
                "      ,[RG_TypeReg] = F_CREGLEMENT_DEST.RG_TypeReg\n" +
                "      ,[RG_Heure] = F_CREGLEMENT_DEST.RG_Heure\n" +
                "      ,[RG_Piece] = F_CREGLEMENT_DEST.RG_Piece\n" +
                "      ,[CA_No] = F_CREGLEMENT_DEST.CA_No\n" +
                "      ,[CO_NoCaissier] = F_CREGLEMENT_DEST.CO_NoCaissier\n" +
                "      ,[RG_Banque] = F_CREGLEMENT_DEST.RG_Banque\n" +
                "      ,[RG_Transfere] = F_CREGLEMENT_DEST.RG_Transfere\n" +
                "      ,[RG_Cloture] = F_CREGLEMENT_DEST.RG_Cloture\n" +
                "      ,[RG_Ticket] = F_CREGLEMENT_DEST.RG_Ticket\n" +
                "      ,[RG_Souche] = F_CREGLEMENT_DEST.RG_Souche\n" +
                "      ,[CT_NumPayeurOrig] = F_CREGLEMENT_DEST.CT_NumPayeurOrig\n" +
                "      ,[RG_DateEchCont] = F_CREGLEMENT_DEST.RG_DateEchCont\n" +
                "      ,[CG_NumEcart] = F_CREGLEMENT_DEST.CG_NumEcart\n" +
                "      ,[JO_NumEcart] = F_CREGLEMENT_DEST.JO_NumEcart\n" +
                "      ,[RG_MontantEcart] = F_CREGLEMENT_DEST.RG_MontantEcart\n" +
                "      ,[RG_NoBonAchat] = F_CREGLEMENT_DEST.RG_NoBonAchat\n" +
                "      ,[cbProt] = F_CREGLEMENT_DEST.cbProt\n" +
                "      ,[cbCreateur] = F_CREGLEMENT_DEST.cbCreateur\n" +
                "      ,[cbModification] = F_CREGLEMENT_DEST.cbModification\n" +
                "      ,[cbReplication] = F_CREGLEMENT_DEST.cbReplication\n" +
                "      ,[cbFlag] = F_CREGLEMENT_DEST.cbFlag\n" +
                "FROM F_CREGLEMENT_DEST\n" +
                "WHERE\tF_CREGLEMENT.RG_NoSource = F_CREGLEMENT_DEST.RG_No\n" +
                "\tAND\tF_CREGLEMENT.DataBaseSource = F_CREGLEMENT_DEST.DataBaseSource\n" +
                "            \n" +
                "INSERT INTO F_CREGLEMENT (\n" +
                "[RG_No],[CT_NumPayeur],[RG_Date],[RG_Reference],[RG_Libelle],[RG_Montant],[RG_MontantDev],[N_Reglement],[RG_Impute]\n" +
                "\t\t,[RG_Compta],[EC_No],[RG_Type],[RG_Cours],[N_Devise],[JO_Num],[CG_NumCont],[RG_Impaye]\n" +
                "\t\t,[CG_Num],[RG_TypeReg],[RG_Heure],[RG_Piece],[CA_No],[CO_NoCaissier],[RG_Banque],[RG_Transfere]\n" +
                "\t\t,[RG_Cloture],[RG_Ticket],[RG_Souche],[CT_NumPayeurOrig],[RG_DateEchCont],[CG_NumEcart],[JO_NumEcart],[RG_MontantEcart]\n" +
                "\t\t,[RG_NoBonAchat],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag],cbMarqSource,DataBaseSource,RG_NoSource)\n" +
                "            \n" +
                "SELECT\tISNULL((SELECT Max(RG_No) FROM F_CREGLEMENT),0) + ROW_NUMBER() OVER(ORDER BY dest.RG_No)\n" +
                "\t\t,[CT_NumPayeur],[RG_Date],[RG_Reference],[RG_Libelle],[RG_Montant],[RG_MontantDev],[N_Reglement],[RG_Impute]\n" +
                "\t\t,[RG_Compta],[EC_No],[RG_Type],[RG_Cours],[N_Devise],[JO_Num],[CG_NumCont],[RG_Impaye]\n" +
                "\t\t,[CG_Num],[RG_TypeReg],[RG_Heure],[RG_Piece],[CA_No],[CO_NoCaissier],[RG_Banque],[RG_Transfere]\n" +
                "\t\t,[RG_Cloture],[RG_Ticket],[RG_Souche],[CT_NumPayeurOrig],[RG_DateEchCont],[CG_NumEcart],[JO_NumEcart],[RG_MontantEcart]\n" +
                "\t\t,[RG_NoBonAchat],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag],[cbMarqSource],dest.[dataBaseSource],dest.RG_No\n" +
                "FROM F_CREGLEMENT_DEST dest\n" +
                "LEFT JOIN (SELECT RG_No,RG_NoSource,dataBaseSource FROM F_CREGLEMENT) src\n" +
                "\tON\tdest.RG_No = src.RG_NoSource\n" +
                "\tAND\tdest.dataBaseSource = src.dataBaseSource\n" +
                "WHERE src.RG_No IS NULL\n" +
                "            \n" +
                "IF OBJECT_ID('F_CREGLEMENT_DEST') IS NOT NULL \n" +
                "DROP TABLE F_CREGLEMENT_DEST;" +
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
                "   'insert',\n" +
                "   'F_REGLEMENT',\n" +
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
        //        readOnFile(path, filename, tableName + "_DEST", sqlCon);
        //        readOnFile(path, "deleteList" + filename, tableName + "_SUPPR", sqlCon);
        //        executeQuery(sqlCon, updateTableDest("", "'RG_No'", tableName, tableName + "_DEST"));
                sendData(sqlCon, path, filename, insert());

        //        deleteTempTable(sqlCon, tableName);
        //        deleteReglement(sqlCon, path,filename);
            }
        }
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"RG_No,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

    public static void getDataElementFilterAgency(Connection sqlCon, String path,String database,String time,String agency)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"RG_No,DatabaseSource");
        getData(sqlCon, selectSourceTableFilterAgencyRegltLink(tableName,database,agency), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

    public static void deleteReglement(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_CREGLEMENT \n" +
                " WHERE EXISTS (SELECT 1 FROM F_CREGLEMENT_SUPPR WHERE F_CREGLEMENT.RG_NoSource = F_CREGLEMENT_SUPPR.RG_No AND F_CREGLEMENT.DataBaseSource = F_CREGLEMENT_SUPPR.DataBaseSource) \n" +
                " \n" +
                " IF OBJECT_ID('F_CREGLEMENT_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_CREGLEMENT_SUPPR \n";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }

}
