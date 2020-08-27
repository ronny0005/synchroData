import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

public class ReglEch extends Table {

    public static String file = "ReglEch.csv";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_REGLECH";
    public static String configList = "listReglEch";

    public static String insert()
    {
        return "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
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
                "            \n" +
                "IF OBJECT_ID('F_REGLECH_DEST') IS NOT NULL \n" +
                "DROP TABLE F_REGLECH_DEST;" +
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
                "   'F_REGLECH',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {
        dbSource = database;
        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "","'RG_No','DR_No'",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,insert());

        deleteTempTable(sqlCon);
        deleteReglEch(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DR_No,RG_No,DataBaseSource");
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList,database);

    }

    public static void deleteTempTable(Connection sqlCon)
    {
        String query = "IF OBJECT_ID('F_REGLECH_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_REGLECH_DEST;";
        executeQuery(sqlCon, query);
    }

    public static void deleteReglEch(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_REGLECH \n" +
                " WHERE EXISTS (SELECT 1 FROM F_REGLECH_SUPPR WHERE F_REGLECH.DataBaseSource = F_REGLECH_SUPPR.DataBaseSource AND F_REGLECH.cbMarqSource = F_REGLECH_SUPPR.cbMarq ) \n" +
                " \n" +
                " IF OBJECT_ID('F_REGLECH_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_REGLECH_SUPPR \n";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }
}
