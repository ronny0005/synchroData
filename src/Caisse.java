import java.sql.Connection;

public class Caisse extends Table {
    public static String file ="caisse_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_CAISSE";
    public static String configList = "listCaisse";

    public static String insert(String filename)
    {
        return
                " BEGIN TRY \n" +
                        "SET DATEFORMAT ymd;\n" +
                        "IF OBJECT_ID('tempdb..#TmpCaisse') IS NOT NULL\n" +
                        "    DROP TABLE #TmpCaisse\n"+
                        "\n" +
                        "SELECT CA_No = ISNULL((SELECT MAX(CA_No) FROM F_CAISSE),0) + ROW_NUMBER() OVER(ORDER BY dest.CA_No) \n" +
                        ",[CA_Intitule],DE_No = ISNULL(dep.DE_No,dest.[DE_No]),[CO_No] \n" +
                        "           ,[CO_NoCaissier],[CT_Num],[JO_Num],[CA_IdentifCaissier] \n" +
                        "           ,[CA_DateCreation],[N_Comptoir],[N_Clavier] \n" +
                        "   ,[CA_LignesAfficheur],[CA_ColonnesAfficheur],[CA_ImpTicket] \n" +
                        "           ,[CA_SaisieVendeur],[CA_Souche],[cbProt],[cbCreateur] \n" +
                        "           ,[cbModification],[cbReplication],[cbFlag] \n" +
                        "   ,CA_NoSource = dest.CA_No, dest.cbMarqSource,dest.DatabaseSource,[CA_NoSourceCai] = src.CA_NoSource \n" +
                        "   INTO #TmpCaisse \n" +
                        "FROM F_CAISSE_DEST dest \n" +
                        "LEFT JOIN (SELECT CA_NoSource,DatabaseSource FROM F_CAISSE) src \n" +
                        "ON src.CA_NoSource = dest.CA_No \n" +
                        "AND src.DatabaseSource = dest.DatabaseSource \n" +
                        "LEFT JOIN (SELECT DE_NoSource,DatabaseSource,DE_No FROM F_DEPOT) dep \n" +
                        "ON dep.DE_NoSource = dest.DE_No \n" +
                        "AND dep.DataBaseSource = dest.DataBaseSource " +
                        "\n" +
                        "UPDATE dwh SET\n" +
                        "      [CA_Intitule] = tmp.CA_Intitule\n" +
                        "      ,[DE_No] = tmp.DE_No\n" +
                        "      ,[CO_No] = tmp.CO_No\n" +
                        "      ,[CO_NoCaissier] = tmp.CO_NoCaissier\n" +
                        "      ,[CT_Num] = tmp.CT_Num\n" +
                        "      ,[JO_Num] = tmp.JO_Num\n" +
                        "      ,[CA_IdentifCaissier] = tmp.CA_IdentifCaissier\n" +
                        "      ,[CA_DateCreation] = tmp.CA_DateCreation\n" +
                        "      ,[N_Comptoir] = tmp.N_Comptoir\n" +
                        "      ,[N_Clavier] = tmp.N_Clavier\n" +
                        "      ,[CA_LignesAfficheur] = tmp.CA_LignesAfficheur\n" +
                        "      ,[CA_ColonnesAfficheur] = tmp.CA_ColonnesAfficheur\n" +
                        "      ,[CA_ImpTicket] = tmp.CA_ImpTicket\n" +
                        "      ,[CA_SaisieVendeur] = tmp.CA_SaisieVendeur\n" +
                        "      ,[CA_Souche] = tmp.CA_Souche\n" +
                        "      ,[cbProt] = tmp.cbProt\n" +
                        "      ,[cbCreateur] = tmp.cbCreateur\n" +
                        "      ,[cbModification] = tmp.cbModification\n" +
                        "      ,[cbReplication] = tmp.cbReplication\n" +
                        "      ,[cbFlag] = tmp.cbFlag\n" +
                        "FROM F_CAISSE dwh\n" +
                        "INNER JOIN #TmpCaisse tmp ON dwh.CA_NoSource = tmp.CA_NoSource\n" +
                        "AND dwh.DatabaseSource = tmp.DatabaseSource\n" +
                        "\n" +
                        "INSERT INTO [dbo].[F_CAISSE]\n" +
                        "           ([CA_No],[CA_Intitule],[DE_No],[CO_No]\n" +
                        "           ,[CO_NoCaissier],[CT_Num],[JO_Num],[CA_IdentifCaissier]\n" +
                        "           ,[CA_DateCreation],[N_Comptoir],[N_Clavier]\n" +
                        "\t\t   ,[CA_LignesAfficheur],[CA_ColonnesAfficheur],[CA_ImpTicket]\n" +
                        "           ,[CA_SaisieVendeur],[CA_Souche],[cbProt],[cbCreateur]\n" +
                        "           ,[cbModification],[cbReplication],[cbFlag]\n" +
                        "\t\t   ,CA_NoSource,cbMarqSource,DatabaseSource)\n" +
                        "\n" +
                        "SELECT [CA_No] \n" +
                        ", [CA_Intitule],[DE_No],[CO_No] \n" +
                        "           ,[CO_NoCaissier],[CT_Num],[JO_Num],[CA_IdentifCaissier] \n" +
                        "           ,[CA_DateCreation],[N_Comptoir],[N_Clavier] \n" +
                        "   ,[CA_LignesAfficheur],[CA_ColonnesAfficheur],[CA_ImpTicket] \n" +
                        "           ,[CA_SaisieVendeur],[CA_Souche],[cbProt],[cbCreateur] \n" +
                        "           ,[cbModification],[cbReplication],[cbFlag] \n" +
                        "   ,CA_NoSource, cbMarqSource,DatabaseSource \n" +
                        "FROM #TmpCaisse \n" +
                        "WHERE [CA_NoSourceCai]  IS NULL " +
                        "\n" +
                        "END TRY" +
                        " BEGIN CATCH  \n" +
                        "INSERT INTO config.DB_Errors \n" +
                        "    VALUES \n" +
                        "  (SUSER_SNAME(), \n" +
                        "   ERROR_NUMBER(), \n" +
                        "   ERROR_STATE(), \n" +
                        "   ERROR_SEVERITY(), \n" +
                        "   ERROR_LINE(), \n" +
                        "   ERROR_PROCEDURE(), \n" +
                        "   ERROR_MESSAGE(), \n" +
                        "   'Insert '+ ' "+filename+"', \n" +
                        "   'F_CAISSE', \n" +
                        "   GETDATE()); \n" +
                        "END CATCH\n" +
                        "\n";
    }

    public static void sendDataElement(Connection sqlCon, String path,String database)
    {
        dbSource = database;
        loadFile(path,sqlCon);
        loadDeleteFile(path,sqlCon);
        DocEntete.loadDeleteFile(path,sqlCon);
    }

    public static void loadDeleteFile(String path,Connection sqlCon) {
        String [] children = getFile(path,"deleteList"+file);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children){
                String deleteCaisse = deleteCaisse();
                loadDeleteInfo(path,tableName,filename,sqlCon,deleteCaisse);
            }
        }
    }

    public static void loadFile(String path,Connection sqlCon){
        String [] children = getFile(path,file);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children){
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                sendData(sqlCon, path, filename, insert(filename));
                //deleteTempTable(sqlCon, tableName+"_DEST");
            }
        }
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"CA_No,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

    public static String deleteCaisse()
    {
        return
                " DELETE src\n" +
                " FROM F_CAISSE src\n" +
                " INNER JOIN F_CAISSE_SUPPR del\n" +
                " ON src.CA_No = del.CA_No ;\n" +
                " AND src.DatabaseSource = del.DatabaseSource ;\n" +
                " IF OBJECT_ID('F_CAISSE_SUPPR') IS NOT NULL\n" +
                " DROP TABLE F_CAISSE_SUPPR ;";
    }

}
