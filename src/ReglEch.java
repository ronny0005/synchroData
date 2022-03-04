import org.json.simple.JSONObject;

import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;
import java.sql.SQLException;

public class ReglEch extends Table {

    public static String file = "ReglEch_";
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
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                readOnFile(path, "deleteList" + filename, tableName + "_SUPPR", sqlCon);
                executeQuery(sqlCon, updateTableDest("", "'RG_No','DR_No'", tableName, tableName + "_DEST"));
                sendData(sqlCon, path, filename, insert());

                deleteTempTable(sqlCon);
                deleteReglEch(sqlCon, path,filename);
            }
        }
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

    public static void deleteReglEch(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_REGLECH \n" +
                " WHERE EXISTS (SELECT 1 FROM F_REGLECH_SUPPR WHERE F_REGLECH.DataBaseSource = F_REGLECH_SUPPR.DataBaseSource AND F_REGLECH.cbMarqSource = F_REGLECH_SUPPR.cbMarq ) \n" +
                " \n" +
                " IF OBJECT_ID('F_REGLECH_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_REGLECH_SUPPR \n";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }
}
