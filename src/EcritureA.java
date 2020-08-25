import java.io.File;
import java.sql.Connection;

public class EcritureA extends Table {

    public static String file = "EcritureA.csv";
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

    public static String insert()
    {
        return  "BEGIN TRY" +
                "\n" +
                "UPDATE\t  [F_ECRITUREA]\n" +
                "   SET\t [EA_Ligne] = F_ECRITUREA_DEST.EA_Ligne\n" +
                "\t\t  ,[CA_Num] = F_ECRITUREA_DEST.CA_Num\n" +
                "\t\t  ,[EA_Montant] = F_ECRITUREA_DEST.EA_Montant\n" +
                "\t\t  ,[EA_Quantite] = F_ECRITUREA_DEST.EA_Quantite\n" +
                "\t\t  ,[cbProt] = F_ECRITUREA_DEST.cbProt\n" +
                "\t\t  ,[cbCreateur] = F_ECRITUREA_DEST.cbCreateur\n" +
                "\t\t  ,[cbModification] = F_ECRITUREA_DEST.cbModification\n" +
                "\t\t  ,[cbReplication] = F_ECRITUREA_DEST.cbReplication\n" +
                "\t\t  ,[cbFlag] = F_ECRITUREA_DEST.cbFlag \n" +
                "FROM\t[F_ECRITUREA_DEST]\n" +
                "WHERE\tF_ECRITUREA.cbMarqSource = F_ECRITUREA_DEST.cbMarqSource\n" +
                "AND\t\tF_ECRITUREA.DatabaseSource = F_ECRITUREA_DEST.DatabaseSource" +
                "                   \n" +
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
                "   'F_ECRITUREA',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }

    public static void deleteTempTable(Connection  sqlCon)
    {
        String query = "IF OBJECT_ID('F_ECRITUREA_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_ECRITUREA_DEST;";
        executeQuery(sqlCon, query);
    }
    public static void sendDataElement(Connection  sqlCon, String path,String database)
    {
        dbSource = database;

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        /*readOnFile(path,file,"F_ECRITUREA_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"F_ECRITUREA_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());

         */
        deleteTempTable(sqlCon);
        deleteEcritureA(sqlCon, path);
    }
    public static void getDataElement(Connection  sqlCon, String path,String database)
    {
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        /*initTable(sqlCon);
        getData(sqlCon, list(), "F_ECRITUREA", path, file);
        listDeleteEcritureA(sqlCon, path);

         */
    }
    public static void initTable(Connection  sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_ECRITUREA') \n" +
                "     INSERT INTO config.ListEcritureA\n" +
                "     SELECT EC_No,DataBaseSource ='" + dbSource + "',cbMarq \n" +
                "     FROM F_ECRITUREA";
        executeQuery(sqlCon, query);
    }
    public static void deleteEcritureA(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_ECRITUREA \n" +
                " WHERE EXISTS (SELECT 1 FROM F_ECRITUREA_SUPPR WHERE F_ECRITUREA_SUPPR.EC_No = F_ECRITUREA.EC_No)  \n" +
                "  \n" +
                " IF OBJECT_ID('F_ECRITUREA_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_ECRITUREA_SUPPR ;";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
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

        writeOnFile(path + "\\deleteList" + file, query, sqlCon);

        query = " DELETE FROM config.ListEcritureA " +
                " WHERE NOT EXISTS(SELECT   1 " +
                "                  FROM     F_ECRITUREA " +
                "                  WHERE    dbo.F_ECRITUREA.cbMarq = config.ListEcritureA.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
