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

    public static String updateCaisse(){
        return  " UPDATE dest SET CA_No = ISNULL(cai.[CA_No],dest.[CA_No]) \n"+
                " FROM F_CREGLEMENT_TMP dest \n" +
                " LEFT JOIN F_CAISSE cai ON ISNULL(cai.CA_NoSource,0) = ISNULL(dest.CA_No,0)\n" +
                " AND ISNULL(cai.dataBaseSource,'') = ISNULL(dest.dataBaseSource,'')  \n";
    }

    public static void sendDataElement(Connection sqlCon, String path,String database,int unibase)
    {
        dbSource = database;
        loadFile(path,sqlCon);
        disableTrigger(sqlCon,tableName);
        loadDeleteFile(path,sqlCon,file,tableName,"RG_No","dataBaseSource");
        enableTrigger(sqlCon,tableName);
//        DocEntete.loadDeleteFile(path,sqlCon);
    }

    public static void loadFile(String path,Connection sqlCon){
        String [] children = getFile(path,file);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children){
                disableTrigger(sqlCon,tableName);
                executeQuery(sqlCon,insertTmpTable (tableName,tableName+"_DEST","RG_No,dataBaseSource",filename,1,1,"RG_No","RG_No","CA_No"));
                executeQuery(sqlCon,updateCaisse());
                executeQuery(sqlCon,insertTable (tableName,tableName+"_TMP","RG_No,dataBaseSource",filename,0,1,"","RG_No",""));
                enableTrigger(sqlCon,tableName);

            }
        }
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"RG_No,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,true), tableName, path, filename);
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

}
