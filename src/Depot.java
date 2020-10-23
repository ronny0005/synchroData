import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Depot extends Table{

    public static String tableName = "F_DEPOT";
    public static String configList = "listDepot";
    public static String file ="depot_";

    public static String insert()
    {
        return          "BEGIN TRY " +
                        " SET DATEFORMAT ymd;\n" +
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
                        "   'insert',\n" +
                        "   'F_DEPOT',\n" +
                        "   GETDATE());\n" +
                        "END CATCH";
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

        File dir = new File(path);
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept (File dir, String name) {
                return name.startsWith(file);
            }
        };
        String[] children = dir.list(filter);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (int i = 0; i< children.length; i++) {
                String filename = children[i];
                readOnFile(path,filename,tableName+"_DEST",sqlCon);
                readOnFile(path,"deleteList"+filename,tableName+"_SUPPR",sqlCon);
                executeQuery(sqlCon,updateTableDest( "DE_No","'DE_No','DP_NoDefaut'",tableName,tableName+"_DEST"));
                sendData(sqlCon, path, filename,insert());

                DepotEmpl.sendDataElement(sqlCon, path,database);
                linkDepot(sqlCon);

                deleteTempTable(sqlCon,tableName);

                deleteDepot(sqlCon, path,filename);
            }
        }

    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"DE_No");
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

        DepotEmpl.getDataElement(sqlCon, path,database, time);
    }

    public static void deleteDepot(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_DEPOT  \n" +
                " WHERE EXISTS (SELECT 1 FROM F_DEPOT_SUPPR WHERE F_DEPOT_SUPPR.DE_No = F_DEPOT.DE_No" +
                "   )  \n" +
                "  \n" +
                " IF OBJECT_ID('F_DEPOT_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_DEPOT_SUPPR ;";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }
}
