import java.sql.Connection;

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

    public static String insert(String filename)
    {
        return "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +

                "IF OBJECT_ID('tempdb..#reglement') IS NOT NULL\n" +
                "    DROP TABLE #reglement\n"+
                " IF OBJECT_ID('F_CREGLEMENT_DEST') IS NOT NULL\n"+
                "UPDATE\tF_CREGLEMENT_DEST\n" +
                "SET\t[RG_Montant] = REPLACE([RG_Montant],',','.')\n" +
                "\t,[RG_MontantDev] = REPLACE([RG_MontantDev],',','.')\n" +
                "\t,[RG_MontantEcart] = REPLACE([RG_MontantEcart],',','.')\n" +
                "\t, [RG_Cours] = REPLACE([RG_Cours],',','.')\n" +
                "\n" +
                "SELECT RG_No = ISNULL((SELECT Max(RG_No) FROM F_CREGLEMENT),0) + ROW_NUMBER() OVER(ORDER BY dest.RG_No) \n" +
                ",dest.[CT_NumPayeur],dest.[RG_Date],dest.[RG_Reference],dest.[RG_Libelle],dest.[RG_Montant],dest.[RG_MontantDev],dest.[N_Reglement],dest.[RG_Impute] \n" +
                ",dest.[RG_Compta],dest.[EC_No],dest.[RG_Type],dest.[RG_Cours],dest.[N_Devise],dest.[JO_Num],dest.[CG_NumCont],dest.[RG_Impaye] \n" +
                ",dest.[CG_Num],dest.[RG_TypeReg],dest.[RG_Heure],dest.[RG_Piece],CA_No = ISNULL(cai.[CA_No],dest.[CA_No]),dest.[CO_NoCaissier],dest.[RG_Banque],dest.[RG_Transfere] \n" +
                ",dest.[RG_Cloture],dest.[RG_Ticket],dest.[RG_Souche],dest.[CT_NumPayeurOrig],dest.[RG_DateEchCont],dest.[CG_NumEcart],dest.[JO_NumEcart],dest.[RG_MontantEcart] \n" +
                ",dest.[RG_NoBonAchat],dest.[cbProt],dest.[cbCreateur],dest.[cbModification],dest.[cbReplication],dest.[cbFlag],dest.[cbMarqSource],dest.[dataBaseSource],RG_NoSource = dest.RG_No \n" +
                ",RG_NoSrc = src.RG_No into #reglement\n" +
                "FROM F_CREGLEMENT_DEST dest \n" +
                "LEFT JOIN (SELECT RG_No,RG_NoSource,dataBaseSource FROM F_CREGLEMENT) src \n" +
                "ON dest.RG_No = src.RG_NoSource \n" +
                "AND dest.dataBaseSource = src.dataBaseSource \n" +
                "LEFT JOIN F_CAISSE cai ON cai.CA_NoSource = dest.CA_No AND cai.dataBaseSource = dest.dataBaseSource  \n" +
                ""+
                " IF OBJECT_ID('F_CREGLEMENT_DEST') IS NOT NULL\n"+
                "UPDATE cre\n" +
                "   SET [CT_NumPayeur] = cre.CT_NumPayeur\n" +
                "      ,[RG_Date] = cre.RG_Date\n" +
                "      ,[RG_Reference] = cre.RG_Reference\n" +
                "      ,[RG_Libelle] = cre.RG_Libelle\n" +
                "      ,[RG_Montant] = cre.RG_Montant\n" +
                "      ,[RG_MontantDev] = cre.RG_MontantDev\n" +
                "      ,[N_Reglement] = cre.N_Reglement\n" +
                "      ,[RG_Impute] = cre.RG_Impute\n" +
                "      ,[RG_Compta] = cre.RG_Compta\n" +
                "      ,[EC_No] = cre.EC_No\n" +
                "      ,[RG_Type] = cre.RG_Type\n" +
                "      ,[RG_Cours] = cre.RG_Cours\n" +
                "      ,[N_Devise] = cre.N_Devise\n" +
                "      ,[JO_Num] = cre.JO_Num\n" +
                "      ,[CG_NumCont] = cre.CG_NumCont\n" +
                "      ,[RG_Impaye] = cre.RG_Impaye\n" +
                "      ,[CG_Num] = cre.CG_Num\n" +
                "      ,[RG_TypeReg] = cre.RG_TypeReg\n" +
                "      ,[RG_Heure] = cre.RG_Heure\n" +
                "      ,[RG_Piece] = cre.RG_Piece\n" +
                "      ,[CA_No] = cre.CA_No\n" +
                "      ,[CO_NoCaissier] = cre.CO_NoCaissier\n" +
                "      ,[RG_Banque] = cre.RG_Banque\n" +
                "      ,[RG_Transfere] = cre.RG_Transfere\n" +
                "      ,[RG_Cloture] = cre.RG_Cloture\n" +
                "      ,[RG_Ticket] = cre.RG_Ticket\n" +
                "      ,[RG_Souche] = cre.RG_Souche\n" +
                "      ,[CT_NumPayeurOrig] = cre.CT_NumPayeurOrig\n" +
                "      ,[RG_DateEchCont] = cre.RG_DateEchCont\n" +
                "      ,[CG_NumEcart] = cre.CG_NumEcart\n" +
                "      ,[JO_NumEcart] = cre.JO_NumEcart\n" +
                "      ,[RG_MontantEcart] = cre.RG_MontantEcart\n" +
                "      ,[RG_NoBonAchat] = cre.RG_NoBonAchat\n" +
                "      ,[cbProt] = cre.cbProt\n" +
                "      ,[cbCreateur] = cre.cbCreateur\n" +
                "      ,[cbModification] = cre.cbModification\n" +
                "      ,[cbReplication] = cre.cbReplication\n" +
                "      ,[cbFlag] = cre.cbFlag\n" +
                "\t  FROM F_CREGLEMENT cre \n" +
                "\t  INNER JOIN #reglement tmp ON cre.[DataBaseSource] = tmp.[DataBaseSource]\n" +
                "\t  AND cre.RG_NoSource = tmp.RG_NoSource" +
                "            \n" +

                " IF OBJECT_ID('F_CREGLEMENT_DEST') IS NOT NULL\n"+
                "INSERT INTO F_CREGLEMENT (\n" +
                "[RG_No],[CT_NumPayeur],[RG_Date],[RG_Reference],[RG_Libelle],[RG_Montant],[RG_MontantDev],[N_Reglement],[RG_Impute]\n" +
                "\t\t,[RG_Compta],[EC_No],[RG_Type],[RG_Cours],[N_Devise],[JO_Num],[CG_NumCont],[RG_Impaye]\n" +
                "\t\t,[CG_Num],[RG_TypeReg],[RG_Heure],[RG_Piece],[CA_No],[CO_NoCaissier],[RG_Banque],[RG_Transfere]\n" +
                "\t\t,[RG_Cloture],[RG_Ticket],[RG_Souche],[CT_NumPayeurOrig],[RG_DateEchCont],[CG_NumEcart],[JO_NumEcart],[RG_MontantEcart]\n" +
                "\t\t,[RG_NoBonAchat],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag],cbMarqSource,DataBaseSource,RG_NoSource)\n" +
                "            \n" +
                "SELECT\tRG_No\n" +
                "\t\t,[CT_NumPayeur],[RG_Date],[RG_Reference],[RG_Libelle],[RG_Montant],[RG_MontantDev],[N_Reglement],[RG_Impute]\n" +
                "\t\t,[RG_Compta],[EC_No],[RG_Type],[RG_Cours],[N_Devise],[JO_Num],[CG_NumCont],[RG_Impaye]\n" +
                "\t\t,[CG_Num],[RG_TypeReg],[RG_Heure],[RG_Piece],[CA_No],[CO_NoCaissier],[RG_Banque],[RG_Transfere]\n" +
                "\t\t,[RG_Cloture],[RG_Ticket],[RG_Souche],[CT_NumPayeurOrig],[RG_DateEchCont],[CG_NumEcart],[JO_NumEcart],[RG_MontantEcart]\n" +
                "\t\t,[RG_NoBonAchat],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag],[cbMarqSource],[dataBaseSource],[RG_NoSource]\n" +
                "FROM #reglement\n" +
                "WHERE RG_NoSrc IS NULL\n" +
                "            \n" +
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
                "   'F_REGLEMENT',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }

    public static void sendDataElement(Connection sqlCon, String path,String database)
    {
        dbSource = database;
        loadFile(path,sqlCon);
        loadDeleteFile(path,sqlCon);
//        DocEntete.loadDeleteFile(path,sqlCon);
    }

    public static void loadDeleteFile(String path,Connection sqlCon) {
        String [] children = getFile(path,"deleteList"+file);
        disableTrigger(sqlCon,tableName);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children){
                String deleteReglement = deleteReglement();
                loadDeleteInfo(path,tableName,filename,sqlCon,deleteReglement);
            }
        }
        enableTrigger(sqlCon,tableName);
    }

    public static void loadFile(String path,Connection sqlCon){
        String [] children = getFile(path,file);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children){
                disableTrigger(sqlCon,tableName);
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                sendData(sqlCon, path, filename, insert(filename));
                //deleteTempTable(sqlCon, tableName+"_DEST");
                enableTrigger(sqlCon,tableName);
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

    public static String deleteReglement()
    {
        return
                " DELETE FROM F_CREGLEMENT \n" +
                " WHERE EXISTS (SELECT 1 FROM F_CREGLEMENT_SUPPR WHERE F_CREGLEMENT.RG_NoSource = F_CREGLEMENT_SUPPR.RG_No AND F_CREGLEMENT.DataBaseSource = F_CREGLEMENT_SUPPR.DataBaseSource) \n" +
                " \n" +
                " IF OBJECT_ID('F_CREGLEMENT_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_CREGLEMENT_SUPPR \n";
    }

}
