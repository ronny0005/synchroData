import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

public class DocRegl extends Table {

    public static String file = "docRegl.csv";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_DOCREGL";
    public static String configList = "listDocRegl";

    public static String insert()
    {
        return  "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                " UPDATE\tF_DOCREGL_DEST\n" +
                "SET\t[DR_Pourcent] = REPLACE([DR_Pourcent],',','.')\n" +
                "\t,[DR_Montant] = REPLACE([DR_Montant],',','.')\n" +
                "\t, [DR_MontantDev] = REPLACE([DR_MontantDev],',','.')\n" +
                "\n" +
                "UPDATE [dbo].[F_DOCREGL]\n" +
                "   SET [DR_TypeRegl] = F_DOCREGL_DEST.DR_TypeRegl\n" +
                "      ,[DR_Date] = F_DOCREGL_DEST.DR_Date\n" +
                "      ,[DR_Libelle] = F_DOCREGL_DEST.DR_Libelle\n" +
                "      ,[DR_Pourcent] = F_DOCREGL_DEST.DR_Pourcent\n" +
                "      ,[DR_Montant] = F_DOCREGL_DEST.DR_Montant\n" +
                "      ,[DR_MontantDev] = F_DOCREGL_DEST.DR_MontantDev\n" +
                "      ,[DR_Equil] = F_DOCREGL_DEST.DR_Equil\n" +
                "      ,[EC_No] = F_DOCREGL_DEST.EC_No\n" +
                "      ,[DR_Regle] = F_DOCREGL_DEST.DR_Regle\n" +
                "      ,[N_Reglement] = F_DOCREGL_DEST.N_Reglement\n" +
                "      ,[cbProt] = F_DOCREGL_DEST.cbProt\n" +
                "      ,[cbCreateur] = F_DOCREGL_DEST.cbCreateur\n" +
                "      ,[cbModification] = F_DOCREGL_DEST.cbModification\n" +
                "      ,[cbReplication] = F_DOCREGL_DEST.cbReplication\n" +
                "      ,[cbFlag] = F_DOCREGL_DEST.cbFlag\n" +
                "FROM F_DOCREGL_DEST\n" +
                "WHERE\tF_DOCREGL.DR_NoSource = F_DOCREGL_DEST.DR_No\n" +
                "\tAND\tF_DOCREGL.DataBaseSource = F_DOCREGL_DEST.DataBaseSource\n" +
                "            \n" +
                "            \n" +
                "INSERT INTO F_DOCREGL (\n" +
                "[DR_No],[DO_Domaine],[DO_Type],[DO_Piece],[DR_TypeRegl],[DR_Date]\n" +
                "\t\t,[DR_Libelle],[DR_Pourcent],[DR_Montant],[DR_MontantDev],[DR_Equil],[EC_No],[DR_Regle]\n" +
                "\t\t,[N_Reglement],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag],cbMarqSource,DataBaseSource,DR_NoSource)\n" +
                "            \n" +
                "SELECT \tISNULL((SELECT Max(DR_No) FROM F_DOCREGL),0) + ROW_NUMBER() OVER(ORDER BY dest.DR_No)\n" +
                "\t\t,dest.[DO_Domaine],dest.[DO_Type],dest.[DO_Piece],[DR_TypeRegl],[DR_Date]\n" +
                "\t\t,[DR_Libelle],[DR_Pourcent],[DR_Montant],[DR_MontantDev],[DR_Equil],[EC_No],[DR_Regle]\n" +
                "\t\t,[N_Reglement],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag],cbMarqSource,dest.[dataBaseSource],dest.DR_No\n" +
                "FROM F_DOCREGL_DEST dest\n" +
                "LEFT JOIN (SELECT DR_NoSource,DataBaseSource FROM F_DOCREGL) src\n" +
                "\tON\tdest.DR_No = src.DR_NoSource\n" +
                "\tAND\tdest.DataBaseSource = src.DataBaseSource\n" +
                "WHERE src.DR_NoSource IS NULL\n" +
                "            \n" +
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
                "   'insert',\n" +
                "   'F_DOCREGL',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {
        dbSource = database;
        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "","'DR_No'",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,insert());
        deleteTempTable(sqlCon,tableName);

        deleteDocRegl(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DR_No,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList,database);

    }

    public static void deleteDocRegl(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_DOCREGL \n" +
                " WHERE EXISTS (SELECT 1 FROM F_DOCREGL_SUPPR WHERE F_DOCREGL.DataBaseSource = F_DOCREGL_SUPPR.DataBaseSource AND F_DOCREGL.DR_NoSource = F_DOCREGL_SUPPR.DR_No ) \n" +
                " \n" +
                " IF OBJECT_ID('F_DOCENTETE_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_DOCENTETE_SUPPR \n";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }

}
