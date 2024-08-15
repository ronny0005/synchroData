import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class EcritureA extends Table {

    public static String file = "EcritureA_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_ECRITUREA";
    public static String configList = "listEcritureA";

    public static String list()
    {
        return   "SELECT\t[EC_No],[N_Analytique],[EA_Ligne],[CA_Num]\n" +
                "\t\t,[EA_Montant],[EA_Quantite],[cbProt],[cbMarq]\n" +
                "\t\t,[cbCreateur],[cbModification],[cbReplication],[cbFlag]\n" +
                "\t\t,cbMarqSource = [cbMarq]\n" +
                "\t\t,[DataBaseSource] = '" + dbSource + "' \n" +
                "FROM\t[F_ECRITUREA]" +
                "WHERE cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='F_ECRITUREA'),'1900-01-01')";
    }

    public static String insert(String filename)
    {
        return  "BEGIN TRY" +
                "                   \n" +
                " IF OBJECT_ID('F_ECRITUREA_DEST') IS NOT NULL\n"+
                "INSERT INTO F_ECRITUREA ([EC_No],[N_Analytique],[EA_Ligne],[CA_Num] \n" +
                "\t\t,[EA_Montant],[EA_Quantite],[cbProt] \n" +
                "\t\t,[cbCreateur],[cbModification],[cbReplication],[cbFlag] \n" +
                "\t\t,[DataBaseSource],cbMarqSource)\n" +
                "SELECT\tecr.[EC_No],[N_Analytique],[EA_Ligne],[CA_Num] \n" +
                "\t\t,[EA_Montant],[EA_Quantite],[cbProt] \n" +
                "\t\t,[cbCreateur],[cbModification],[cbReplication],[cbFlag] \n" +
                "\t\t,dest.[DataBaseSource],dest.cbMarqSource\n" +
                "FROM\t[F_ECRITUREA_DEST] dest\n" +
                "LEFT JOIN (SELECT cbMarqSource,DataBaseSource FROM F_ECRITUREA) src\n" +
                "\tON\tsrc.cbMarqSource = dest.cbMarqSource\n" +
                "\tAND\t\tsrc.DatabaseSource = dest.DatabaseSource\n" +
                "LEFT JOIN (SELECT EC_No,EC_NoSource,DatabaseSource FROM F_ECRITUREC) ecr\n" +
                "\tON\tecr.EC_NoSource = dest.EC_No\n" +
                "\tAND ecr.DataBaseSource = dest.DataBaseSource" +
                "\nWHERE ecr.EC_No IS NOT NULL\n" +
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
                "   'F_ECRITUREA',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }

    public static void sendDataElement(Connection  sqlCon, String path,String database,int unibase)
    {
        File dir = new File(path);
        FilenameFilter filter = (dir1, name) -> name.startsWith(file);
        String[] children = dir.list(filter);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                dbSource = database;
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                readOnFile(path, "deleteList" + filename, tableName + "_SUPPR", sqlCon);
                executeQuery(sqlCon, updateTableDest("", "'EC_No','N_Analytique'", tableName, tableName + "_DEST", filename,unibase));
                sendData(sqlCon, path, filename, insert(filename));
                deleteTempTable(sqlCon, tableName + "_DEST");
                deleteEcritureA(sqlCon, path, filename);
            }
        }
    }
    public static void getDataElement(Connection  sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"EC_No,DataBaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,true), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }
    public static void deleteEcritureA(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_ECRITUREA \n" +
                " WHERE EXISTS (SELECT 1 FROM F_ECRITUREA_SUPPR WHERE F_ECRITUREA_SUPPR.EC_No = F_ECRITUREA.EC_No)  \n" +
                "  \n" +
                " IF OBJECT_ID('F_ECRITUREA_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_ECRITUREA_SUPPR ;";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }

    public static void listDeleteEcritureA(Connection sqlCon, String path)
    {
        String query = " SELECT lart.EC_No,lart.cbMarq " +
                " FROM config.ListEcritureA lart " +
                " LEFT JOIN dbo.F_ECRITUREA fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeToFileAvro(path + "\\deleteList" + file, query, sqlCon);
        writeOnFile(path + "\\deleteList" + file, query, sqlCon);

        query = " DELETE FROM config.ListEcritureA " +
                " WHERE NOT EXISTS(SELECT   1 " +
                "                  FROM     F_ECRITUREA " +
                "                  WHERE    dbo.F_ECRITUREA.cbMarq = config.ListEcritureA.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
