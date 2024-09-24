import org.json.simple.JSONObject;

import java.sql.Connection;

public class ReglEch extends Table {

    public static String file = "ReglEch_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_REGLECH";
    public static String configList = "listReglEch";

    public static String insert(String filename)
    {
        return "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +

                "            \n" +

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

                "SELECT\t   SUSER_SNAME()," +
                "           NULL," +
                "           NULL," +
                "           NULL," +
                "           NULL," +
                "           NULL," +
                "           NULL,'Domaine : '+CAST(dest.[DO_Domaine] AS VARCHAR(150))+' Type : ' + CAST(dest.[DO_Type] AS VARCHAR(150)) + ' Piece : ' + CAST(dest.[DO_Piece] AS VARCHAR(150))" +
                "           +' cbMarq : ' + CAST(dest.[cbMarqSource] AS VARCHAR(150)) + ' database : ' + CAST(dest.[DataBaseSource] AS VARCHAR(150))" +
                "           + 'fileName : "+filename+" RG_No : '+CAST(dest.RG_No AS VARCHAR(150))+' DR_No : '+CAST(dest.DR_No AS VARCHAR(150))  " +
                "           ,'F_REGLECH'\n" +
                "           ,GETDATE()\n" +
                "FROM F_REGLECH_DEST dest\n" +
                "LEFT JOIN (SELECT DataBaseSource,cbMarqSource FROM F_REGLECH) src\n" +
                "\tON\tISNULL(dest.DataBaseSource,'') = ISNULL(src.DataBaseSource,'')\n" +
                "\tAND ISNULL(dest.cbMarqSource,0) = ISNULL(src.cbMarqSource,0)\n" +
                "LEFT JOIN F_DOCREGL fdr\n" +
                "\tON\tISNULL(fdr.DataBaseSource,'') = ISNULL(dest.DataBaseSource,'')\n" +
                "\tAND ISNULL(fdr.DR_NoSource,0) = ISNULL(dest.DR_No,0)\n" +
                "LEFT JOIN F_CREGLEMENT cre\n" +
                "\tON\tISNULL(cre.DataBaseSource,'') = ISNULL(dest.DataBaseSource,'')\n" +
                "\tAND ISNULL(cre.RG_NoSource,0) = ISNULL(dest.RG_No,0)\n" +
                "WHERE src.cbMarqSource IS NULL\n" +
                "AND (cre.RG_No IS NULL OR fdr.DR_No IS NULL)\n" +
                "\n" +
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
                "   'Insert "+filename+"',\n" +
                "   'F_REGLECH',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }

    public static String linkDrRGNo (){
        return "UPDATE dest SET RG_No = cre.[RG_No]\n" +
                "\t\t\t\t,DR_No = fdr.[DR_No]\n" +
                "FROM F_REGLECH_DEST dest\n" +
                "LEFT JOIN F_DOCREGL fdr\n" +
                "ON ISNULL(fdr.DataBaseSource,'') = ISNULL(dest.DataBaseSource,'')\n" +
                "AND ISNULL(fdr.DR_NoSource,0) = ISNULL(dest.DR_No,0)\n" +
                "LEFT JOIN F_CREGLEMENT cre\n" +
                "ON ISNULL(cre.DataBaseSource,'') = ISNULL(dest.DataBaseSource,'')\n" +
                "AND ISNULL(cre.RG_NoSource,0) = ISNULL(dest.RG_No,0)\n" +
                "WHERE cre.RG_No IS NOT NULL AND fdr.DR_No IS NOT NULL;\n" +
                "\n" +
                "DELETE FROM F_REGLECH_DEST \n" +
                "WHERE RG_No IS NULL OR DR_No IS NULL;";
    }
    public static void sendDataElement(Connection sqlCon, String path,String database,int unibase)
    {
        dbSource = database;
        loadFile(path,sqlCon,unibase);
        loadDeleteFile(path,sqlCon);
        DocEntete.loadDeleteFile(path,sqlCon);
    }

    public static void loadFile(String path,Connection sqlCon,int unibase){
        String [] children = getFile(path,file);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                disableTrigger(sqlCon,tableName);
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                executeQuery(sqlCon, updateTableDest("", "'RG_No','DR_No'", tableName, tableName + "_DEST",filename,unibase));
                sendData(sqlCon, path, filename, insert(filename));
                executeQuery(sqlCon,linkDrRGNo());
                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","cbMarqSource,dataBaseSource",filename,0,1,"","cbMarqSource,DR_NoSource",""));
            //    deleteTempTable(sqlCon, tableName+"_DEST");
                enableTrigger(sqlCon,tableName);
            }
        }
    }

    public static void loadDeleteFile(String path,Connection sqlCon) {
        String [] children = getFile(path,"deleteList"+file);
        disableTrigger(sqlCon,tableName);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                String deleteReglEch = deleteReglEch();
                loadDeleteInfo(path,tableName,filename,sqlCon,deleteReglEch);
            }
        }
        enableTrigger(sqlCon,tableName);
    }

    public static void getDataElement(Connection sqlCon, String path, String database, String time, JSONObject type)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DR_No,RG_No,DataBaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,type), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }
    public static void getDataElementFilterAgency(Connection sqlCon, String path,String database,String time,String agency)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DR_No,RG_No,DataBaseSource");
        getData(sqlCon, selectSourceTableFilterAgencyEnteteLink(tableName,database,agency), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }

    public static void deleteTempTable(Connection sqlCon)
    {
        String query = "IF OBJECT_ID('F_REGLECH_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_REGLECH_DEST;";
        executeQuery(sqlCon, query);
    }

    public static String deleteReglEch()
    {
        return
                " DELETE FROM F_REGLECH \n" +
                " WHERE EXISTS (SELECT 1 FROM F_REGLECH_SUPPR WHERE ISNULL(F_REGLECH.DataBaseSource,'') = ISNULL(F_REGLECH_SUPPR.DataBaseSource,'') AND ISNULL(F_REGLECH.cbMarqSource,0) = ISNULL(F_REGLECH_SUPPR.cbMarq,0) ) \n" +
                " \n" +
                " IF OBJECT_ID('F_REGLECH_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_REGLECH_SUPPR \n";
    }
}
