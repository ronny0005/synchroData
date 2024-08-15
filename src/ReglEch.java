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

                " IF OBJECT_ID('F_REGLECH_DEST') IS NOT NULL\n"+
                "INSERT INTO F_REGLECH (\n" +
                "[RG_No],[DR_No],[DO_Domaine],[DO_Type]\n" +
                "\t\t,[DO_Piece],[RC_Montant],[RG_TypeReg],[cbProt],[cbCreateur]\n" +
                "\t\t,[cbModification],[cbReplication],[cbFlag],cbMarqSource,DataBaseSource,DR_NoSource,RG_NoSource)\n" +
                "            \n" +
                "SELECT \tcre.[RG_No],fdr.[DR_No],dest.[DO_Domaine],dest.[DO_Type]\n" +
                "\t\t,dest.[DO_Piece],dest.[RC_Montant],dest.[RG_TypeReg],dest.[cbProt],dest.[cbCreateur]\n" +
                "\t\t,dest.[cbModification],dest.[cbReplication],dest.[cbFlag],dest.cbMarqSource,dest.DataBaseSource,dest.DR_No,dest.RG_No\n" +
                "FROM F_REGLECH_DEST dest\n" +
                "LEFT JOIN (SELECT DataBaseSource,cbMarqSource FROM F_REGLECH) src\n" +
                "\tON\tdest.DataBaseSource = src.DataBaseSource\n" +
                "\tAND dest.cbMarqSource = src.cbMarqSource\n" +
                "LEFT JOIN F_DOCREGL fdr\n" +
                "\tON\tfdr.DataBaseSource = dest.DataBaseSource\n" +
                "\tAND fdr.DR_NoSource = dest.DR_No\n" +
                "LEFT JOIN F_CREGLEMENT cre\n" +
                "\tON\tcre.DataBaseSource = dest.DataBaseSource\n" +
                "\tAND cre.RG_NoSource = dest.RG_No\n" +
                "WHERE src.cbMarqSource IS NULL\n" +
                "AND cre.RG_No IS NOT NULL AND fdr.DR_No IS NOT NULL\n" +
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
                "\tON\tdest.DataBaseSource = src.DataBaseSource\n" +
                "\tAND dest.cbMarqSource = src.cbMarqSource\n" +
                "LEFT JOIN F_DOCREGL fdr\n" +
                "\tON\tfdr.DataBaseSource = dest.DataBaseSource\n" +
                "\tAND fdr.DR_NoSource = dest.DR_No\n" +
                "LEFT JOIN F_CREGLEMENT cre\n" +
                "\tON\tcre.DataBaseSource = dest.DataBaseSource\n" +
                "\tAND cre.RG_NoSource = dest.RG_No\n" +
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
                " WHERE EXISTS (SELECT 1 FROM F_REGLECH_SUPPR WHERE F_REGLECH.DataBaseSource = F_REGLECH_SUPPR.DataBaseSource AND F_REGLECH.cbMarqSource = F_REGLECH_SUPPR.cbMarq ) \n" +
                " \n" +
                " IF OBJECT_ID('F_REGLECH_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_REGLECH_SUPPR \n";
    }
}
