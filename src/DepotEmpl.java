import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class DepotEmpl extends Table {

    public static String file = "depotEmpl_";
    public static String tableName = "F_DEPOTEMPL";
    public static String configList = "listDepotEmpl";

    public static String insert(String filename)
    {
        return          "BEGIN TRY " +
                        " SET DATEFORMAT ymd;\n" +

                        " IF OBJECT_ID('F_DEPOTEMPL_DEST') IS NOT NULL\n"+
                        " UPDATE [dbo].[F_DEPOTEMPL]  \n" +
                        "   SET [DP_Code] = F_DEPOTEMPL_DEST.DP_Code\n" +
                        "      ,[DP_Intitule] = F_DEPOTEMPL_DEST.DP_Intitule\n" +
                        "      ,[DP_Zone] = F_DEPOTEMPL_DEST.DP_Zone\n" +
                        "      ,[DP_Type] = F_DEPOTEMPL_DEST.DP_Type\n" +
                        "      ,[cbProt] = F_DEPOTEMPL_DEST.cbProt\n" +
                        "      ,[cbCreateur] = F_DEPOTEMPL_DEST.cbCreateur\n" +
                        "      ,[cbModification] = F_DEPOTEMPL_DEST.cbModification\n" +
                        "      ,[cbReplication] = F_DEPOTEMPL_DEST.cbReplication\n" +
                        "      ,[cbFlag] = F_DEPOTEMPL_DEST.cbFlag\n" +
                        "FROM F_DEPOTEMPL_DEST       \n" +
                        "  WHERE F_DEPOTEMPL.DP_NoSource = F_DEPOTEMPL_DEST.DP_No \n" +
                        "  AND\tF_DEPOTEMPL.DataBaseSource = F_DEPOTEMPL_DEST.DataBaseSource \n" +
                        "  \n" +
                        "  INSERT INTO [dbo].[F_DEPOTEMPL]     \n" +
                        "  ([DP_No],[DE_No],[DP_Code],[DP_Intitule],[DP_Zone],[DP_Type],[cbProt]\n" +
                        "      ,[cbCreateur],[cbModification],[cbReplication],[cbFlag],DP_NoSource,cbMarqSource,DataBaseSource)     \n" +
                        "     \n" +
                        "  SELECT ISNULL((SELECT Max(DP_No) FROM F_DEPOTEMPL),0)  + ROW_NUMBER() OVER(ORDER BY dest.DP_No),srcDep.[DE_No],[DP_Code],[DP_Intitule],[DP_Zone],[DP_Type],[cbProt]\n" +
                        "      ,[cbCreateur],[cbModification],[cbReplication],[cbFlag],dest.DP_No,dest.cbMarqSource,dest.DataBaseSource \n" +
                        "  FROM F_DEPOTEMPL_DEST dest       \n" +
                        "  LEFT JOIN (SELECT DP_NoSource,DataBaseSource FROM F_DEPOTEMPL) src       \n" +
                        "  ON dest.DP_No = src.DP_NoSource\n" +
                        "  AND dest.DataBaseSource = src.DataBaseSource\n" +
                        "  LEFT JOIN (SELECT DatabaseSource,DE_NoSource,DE_No FROM F_DEPOT) srcDep       \n" +
                        "  ON\tdest.DE_No = srcDep.DE_NoSource\n" +
                        "  AND\tdest.DataBaseSource = srcDep.DataBaseSource\n" +
                        "  WHERE src.DP_NoSource IS NULL \n" +
                        "  AND\tsrcDep.DE_No IS NOT NULL;   " +
                        "  IF OBJECT_ID('F_DEPOTEMPL_DEST') IS NOT NULL     \n" +
                        "  DROP TABLE F_DEPOTEMPL_DEST;" +
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
                        "   'F_DEPOTEMPL',\n" +
                        "   GETDATE());\n" +
                        "END CATCH";
    }

    public static void sendDataElement(Connection sqlCon, String path,int unibase)
    {
        File dir = new File(path);
        FilenameFilter filter = (dir1, name) -> name.startsWith(file);
        String[] children = dir.list(filter);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                readOnFile(path, "deleteList" + filename, tableName + "_SUPPR", sqlCon);
                executeQuery(sqlCon, updateTableDest("", "'DP_No','DE_No','DP_NoSource'", tableName, tableName + "_DEST", filename,unibase));
                sendData(sqlCon, path, filename, insert(filename));

                deleteTempTable(sqlCon, tableName + "_DEST");
                deleteDepot(sqlCon, path, filename);
            }
        }
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"DP_No,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,true), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }
    public static void deleteDepot(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_DEPOTEMPL  \n" +
                " WHERE EXISTS (SELECT 1 " +
                "               FROM F_DEPOTEMPL_SUPPR emplSuppr" +
                "               WHERE emplSuppr.DP_No = F_DEPOTEMPL.DP_NoSource " +
                "               AND emplSuppr.DataBaseSource = F_DEPOTEMPL.DataBaseSource" +
                "   )  \n" +
                "  \n" +
                " IF OBJECT_ID('F_DEPOTEMPL_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_DEPOTEMPL_SUPPR ;";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }
}
