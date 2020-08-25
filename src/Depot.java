import java.io.File;
import java.sql.Connection;

public class Depot extends Table{

    public static String file ="depot.csv";
    public static String tableName = "F_DEPOT";
    public static String configList = "listDepot";

    public static String list()
    {
        return "SELECT\t[DE_No],[DE_Intitule],[DE_Adresse],[DE_Complement],[DE_CodePostal],[DE_Ville]\n" +
                "\t\t,[DE_Contact],[DE_Principal],[DE_CatCompta],[DE_Region],[DE_Pays],[DE_EMail]\n" +
                "\t\t,[DE_Code],[DE_Telephone],[DE_Telecopie],[DE_Replication],[DP_NoDefaut],[cbProt]\n" +
                "\t\t,[cbCreateur],[cbModification],[cbReplication],[cbFlag]\n" +
                ", cbMarqSource = cbMarq \n" +
                "FROM\t[F_DEPOT]\n" +
                "WHERE cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='F_DEPOT'),'1900-01-01')";
    }

    public static String insert()
    {
        return          "BEGIN TRY " +
                        " SET DATEFORMAT ymd;\n" +
                        "   UPDATE [dbo].[F_DEPOT] \n" +
                        "   SET [DE_Intitule] = [F_DEPOT_DEST].DE_Intitule\n" +
                        "      ,[DE_Adresse] = [F_DEPOT_DEST].DE_Adresse\n" +
                        "      ,[DE_Complement] = [F_DEPOT_DEST].DE_Complement\n" +
                        "      ,[DE_CodePostal] = [F_DEPOT_DEST].DE_CodePostal\n" +
                        "      ,[DE_Ville] = [F_DEPOT_DEST].DE_Ville\n" +
                        "      ,[DE_Contact] = [F_DEPOT_DEST].DE_Contact\n" +
                        "      ,[DE_Principal] = [F_DEPOT_DEST].DE_Principal\n" +
                        "      ,[DE_CatCompta] = [F_DEPOT_DEST].DE_CatCompta\n" +
                        "      ,[DE_Region] = [F_DEPOT_DEST].DE_Region\n" +
                        "      ,[DE_Pays] = [F_DEPOT_DEST].DE_Pays\n" +
                        "      ,[DE_EMail] = [F_DEPOT_DEST].DE_EMail\n" +
                        "      ,[DE_Code] = [F_DEPOT_DEST].DE_Code\n" +
                        "      ,[DE_Telephone] = [F_DEPOT_DEST].DE_Telephone\n" +
                        "      ,[DE_Telecopie] = [F_DEPOT_DEST].DE_Telecopie\n" +
                        "      ,[DE_Replication] = [F_DEPOT_DEST].DE_Replication\n" +
                        //"      ,[DP_NoDefaut] = [F_DEPOT_DEST].DP_NoDefaut\n" +
                        "      ,[cbProt] = [F_DEPOT_DEST].cbProt\n" +
                        "      ,[cbCreateur] = [F_DEPOT_DEST].cbCreateur\n" +
                        "      ,[cbModification] = [F_DEPOT_DEST].cbModification\n" +
                        "      ,[cbReplication] = [F_DEPOT_DEST].cbReplication\n" +
                        "      ,[cbFlag] = [F_DEPOT_DEST].cbFlag\n" +
                        "FROM F_DEPOT_DEST      \n" +
                        "  WHERE F_DEPOT.DE_No = F_DEPOT_DEST.DE_No\n" +
                        " \n" +
                        "  INSERT INTO [dbo].[F_DEPOT]    \n" +
                        "  ([DE_No],[DE_Intitule],[DE_Adresse],[DE_Complement],[DE_CodePostal],[DE_Ville]\n" +
                        "\t\t,[DE_Contact],[DE_Principal],[DE_CatCompta],[DE_Region],[DE_Pays],[DE_EMail]\n" +
                        "\t\t,[DE_Code],[DE_Telephone],[DE_Telecopie],[DE_Replication],[DP_NoDefaut],[cbProt]\n" +
                        "\t\t,[cbCreateur],[cbModification],[cbReplication],[cbFlag])    \n" +
                        "    \n" +
                        "  SELECT dest.[DE_No],[DE_Intitule],[DE_Adresse],[DE_Complement],[DE_CodePostal],[DE_Ville]\n" +
                        "\t\t,[DE_Contact],[DE_Principal],[DE_CatCompta],[DE_Region],[DE_Pays],[DE_EMail]\n" +
                        "\t\t,[DE_Code],[DE_Telephone],[DE_Telecopie],[DE_Replication],null/*[DP_NoDefaut]*/,[cbProt]\n" +
                        "\t\t,[cbCreateur],[cbModification],[cbReplication],[cbFlag] \n" +
                        "  FROM F_DEPOT_DEST dest      \n" +
                        "  LEFT JOIN (SELECT [DE_No] FROM F_DEPOT) src      \n" +
                        "  ON dest.DE_No = src.DE_No      \n" +
                        "  WHERE src.DE_No IS NULL ;    \n" +
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
                        "   'F_DEPOT',\n" +
                        "   GETDATE());\n" +
                        "END CATCH";
    }

    public static void deleteTempTable(Connection sqlCon)
    {
        String query = "IF OBJECT_ID('F_DEPOT_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_DEPOT_DEST;";
        executeQuery(sqlCon, query);
    }
    public static void linkDepot(Connection sqlCon)
    {
        String query = "UPDATE F_DEPOT \n" +
                "     SET DP_NoDefaut = F_DEPOT_DEST.DP_NoDefaut \n" +
                " FROM F_DEPOT_DEST \n" +
                " WHERE F_DEPOT_DEST.DE_No = F_DEPOT.DE_No \n";
        //                           " UPDATE F_DEPOTEMPL \n" +
        //                           "     SET DE_No = F_DEPOTEMPL_DEST.DE_No \n" +
        //" FROM F_DEPOTEMPL_DEST \n" +
        //" WHERE F_DEPOTEMPL_DEST.DP_No = F_DEPOTEMPL.DP_No";
        executeQuery(sqlCon, query);
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        /*readOnFile(path,file,"F_DEPOT_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"F_DEPOT_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());

         */
        DepotEmpl.sendDataElement(sqlCon, path,database);
        linkDepot(sqlCon);
        deleteTempTable(sqlCon);
        deleteDepot(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        /*initTable(sqlCon);
        getData(sqlCon, list(), "F_DEPOT", path, file);
        listDeleteDepot(sqlCon, path);

         */
        DepotEmpl.getDataElement(sqlCon, path,database);
    }
    public static void initTable(Connection sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_DEPOT') \n" +
                "     INSERT INTO config.ListDepot\n" +
                "     SELECT DE_No,cbMarq \n" +
                "     FROM F_DEPOT";
        executeQuery(sqlCon, query);
    }
    public static void deleteDepot(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_DEPOT  \n" +
                " WHERE EXISTS (SELECT 1 FROM F_DEPOT_SUPPR WHERE F_DEPOT_SUPPR.DE_No = F_DEPOT.DE_No" +
                "   )  \n" +
                "  \n" +
                " IF OBJECT_ID('F_DEPOT_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_DEPOT_SUPPR ;";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }

    public static void listDeleteDepot(Connection sqlCon, String path)
    {
        String query = " SELECT lart.DE_No,lart.cbMarq " +
                " FROM config.ListDepot lart " +
                " LEFT JOIN dbo.F_DEPOT fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\deleteList" + file, query, sqlCon);

        query = " DELETE FROM config.ListDepot " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_DEPOT " +
                "                  WHERE dbo.F_DEPOT.cbMarq = config.ListDepot.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
