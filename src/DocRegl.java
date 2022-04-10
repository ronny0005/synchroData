import org.json.simple.JSONObject;

import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;
import java.sql.SQLException;

public class DocRegl extends Table {

    public static String file = "docRegl_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_DOCREGL";
    public static String configList = "listDocRegl";

    public static String insert(String filename)
    {
        return  "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +

                " IF OBJECT_ID('F_DOCREGL_DEST') IS NOT NULL\n"+
                " UPDATE\tF_DOCREGL_DEST\n" +
                "SET\t[DR_Pourcent] = REPLACE([DR_Pourcent],',','.')\n" +
                "\t,[DR_Montant] = REPLACE([DR_Montant],',','.')\n" +
                "\t, [DR_MontantDev] = REPLACE([DR_MontantDev],',','.')\n" +
                "\n" +

                " IF OBJECT_ID('F_DOCREGL_DEST') IS NOT NULL\n"+
                "UPDATE src\n" +
                "   SET [DR_TypeRegl] = dest.DR_TypeRegl\n" +
                "      ,[DR_Date] = dest.DR_Date\n" +
                "      ,[DR_Libelle] = dest.DR_Libelle\n" +
                "      ,[DR_Pourcent] = dest.DR_Pourcent\n" +
                "      ,[DR_Montant] = dest.DR_Montant\n" +
                "      ,[DR_MontantDev] = dest.DR_MontantDev\n" +
                "      ,[DR_Equil] = dest.DR_Equil\n" +
                "      ,[EC_No] = dest.EC_No\n" +
                "      ,[DR_Regle] = dest.DR_Regle\n" +
                "      ,[N_Reglement] = dest.N_Reglement\n" +
                "      ,[cbProt] = dest.cbProt\n" +
                "      ,[cbCreateur] = dest.cbCreateur\n" +
                "      ,[cbModification] = dest.cbModification\n" +
                "      ,[cbReplication] = dest.cbReplication\n" +
                "      ,[cbFlag] = dest.cbFlag\n" +
                "      ,[DR_NoSource] = dest.DR_No\n" +
                "FROM [dbo].[F_DOCREGL] src\n" +
                "INNER JOIN F_DOCREGL_DEST dest ON src.cbMarqSource = dest.cbMarqSource \n" +
                "\tAND\tsrc.DataBaseSource = dest.DataBaseSource\n" +
                "            \n" +

                " IF OBJECT_ID('F_DOCREGL_DEST') IS NOT NULL\n"+
                "INSERT INTO F_DOCREGL (\n" +
                "[DR_No],[DO_Domaine],[DO_Type],[DO_Piece],[DR_TypeRegl],[DR_Date]\n" +
                "\t\t,[DR_Libelle],[DR_Pourcent],[DR_Montant],[DR_MontantDev],[DR_Equil],[EC_No],[DR_Regle]\n" +
                "\t\t,[N_Reglement],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag],cbMarqSource,DataBaseSource,DR_NoSource)\n" +
                "            \n" +
                "SELECT \tISNULL((SELECT Max(DR_No) FROM F_DOCREGL),0) + ROW_NUMBER() OVER(ORDER BY dest.DR_No)\n" +
                "\t\t,dest.[DO_Domaine],dest.[DO_Type],dest.[DO_Piece],[DR_TypeRegl],[DR_Date]\n" +
                "\t\t,[DR_Libelle],[DR_Pourcent],[DR_Montant],[DR_MontantDev],[DR_Equil],[EC_No],[DR_Regle]\n" +
                "\t\t,[N_Reglement],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag],dest.cbMarqSource,dest.[dataBaseSource],dest.DR_No\n" +
                "FROM F_DOCREGL_DEST dest\n" +
                "LEFT JOIN (SELECT cbMarqSource,DataBaseSource FROM F_DOCREGL) src\n" +
                "\tON\tdest.cbMarqSource = src.cbMarqSource\n" +
                "\tAND\tdest.DataBaseSource = src.DataBaseSource\n" +
                "LEFT JOIN (SELECT DO_Piece,DO_Type,DO_Domaine FROM F_DOCENTETE) docE\n" +
                "ON docE.DO_Domaine = dest.DO_Domaine\n"+
                "AND docE.DO_Type = dest.DO_Type\n"+
                "AND docE.DO_Piece = dest.DO_Piece\n"+
                "WHERE src.cbMarqSource IS NULL\n" +
                "AND docE.DO_Piece IS NOT NULL\n" +
                "            \n" +
                "" +
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
                "           NULL,'Domaine : '+dest.[DO_Domaine]+' Type : ' + dest.[DO_Type] + ' Piece : ' + dest.[DO_Piece]" +
                "           +' cbMarq : ' + dest.[cbMarqSource] + ' database : ' + dest.[DataBaseSource]" +
                "           + 'fileName : "+filename+" '" +
                "           ,'F_DOCLIGNE'\n" +
                "           ,GETDATE()\n" +
                "FROM F_DOCREGL_DEST dest\n" +
                "LEFT JOIN (SELECT cbMarqSource,DataBaseSource FROM F_DOCREGL) src\n" +
                "\tON\tdest.cbMarqSource = src.cbMarqSource\n" +
                "\tAND\tdest.DataBaseSource = src.DataBaseSource\n" +
                "LEFT JOIN (SELECT DO_Piece,DO_Type,DO_Domaine FROM F_DOCENTETE) docE\n" +
                "ON docE.DO_Domaine = dest.DO_Domaine\n"+
                "AND docE.DO_Type = dest.DO_Type\n"+
                "AND docE.DO_Piece = dest.DO_Piece\n"+
                "WHERE src.cbMarqSource IS NULL\n" +
                "AND docE.DO_Piece IS NULL\n" +
                "IF OBJECT_ID('F_DOCREGL_DEST') IS NOT NULL \n" +
                "DROP TABLE F_DOCREGL_DEST\n" +
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
                "   'F_DOCREGL',\n" +
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
                executeQuery(sqlCon, updateTableDest("", "'DR_No'", tableName, tableName + "_DEST",filename));
                sendData(sqlCon, path, filename, insert(filename));
                deleteTempTable(sqlCon, tableName);
                deleteDocRegl(sqlCon, path,filename);
            }
        }
    }
    public static void getDataElement(Connection sqlCon, String path, String database, String time, JSONObject type)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DR_No,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,type), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }
    public static void getDataElementFilterAgency(Connection sqlCon, String path,String database,String time,String agency)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DR_No,DatabaseSource");
        getData(sqlCon, selectSourceTableFilterAgencyEnteteLink(tableName,database,agency), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }

    public static void deleteDocRegl(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_DOCREGL \n" +
                " WHERE EXISTS (SELECT 1 FROM F_DOCREGL_SUPPR WHERE F_DOCREGL.DataBaseSource = F_DOCREGL_SUPPR.DataBaseSource AND F_DOCREGL.DR_NoSource = F_DOCREGL_SUPPR.DR_No ) \n" +
                " \n" +
                " IF OBJECT_ID('F_DOCENTETE_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_DOCENTETE_SUPPR \n";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }

}
