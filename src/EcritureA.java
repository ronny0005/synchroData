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
                "\tON\tISNULL(src.cbMarqSource,0) = ISNULL(dest.cbMarqSource,0)\n" +
                "\tAND\t\tISNULL(src.DatabaseSource,'') = ISNULL(dest.DatabaseSource,'')\n" +
                "LEFT JOIN (SELECT EC_No,EC_NoSource,DatabaseSource FROM F_ECRITUREC) ecr\n" +
                "\tON\tISNULL(ecr.EC_NoSource,0) = ISNULL(dest.EC_No,0)\n" +
                "\tAND ISNULL(ecr.DataBaseSource,'') = ISNULL(dest.DataBaseSource,'')" +
                "\nWHERE ecr.EC_No IS NOT NULL\n" +
                "\nAND src.cbMarqSource IS NULL\n" +
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

    public static String updateECNo (){
        return  "UPDATE tmp SET EC_No = ISNULL(ecr.EC_No,dest.[EC_No])" +
                "FROM\tF_ECRITUREA_TMP tmp\n" +
                "INNER JOIN\tF_ECRITUREA_DEST dest ON dest.cbMarqSource = tmp.cbMarqSource\n" +
                "LEFT JOIN F_ECRITUREC ecr\n" +
                "\tON\tISNULL(ecr.EC_NoSource,0) = ISNULL(dest.EC_No,0)\n" +
                "\tAND ISNULL(ecr.DataBaseSource,'') = ISNULL(dest.DataBaseSource,'');" +
                "\nDELETE FROM [F_ECRITUREA_TMP]\n" +
                "\nWHERE EC_No IS NULL;\n";
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

                disableTrigger(sqlCon,tableName);
                executeQuery(sqlCon, updateTableDest("", "EC_No,N_Analytique", tableName, tableName + "_DEST", filename,unibase));

                executeQuery(sqlCon,insertTmpTable (tableName,tableName+"_DEST","cbMarqSource,dataBaseSource",filename,0,0,"","","EC_No"));
                executeQuery(sqlCon,updateECNo());
                executeQuery(sqlCon,insertTable (tableName,tableName+"_TMP","cbMarqSource,dataBaseSource",filename,0,0,"","",""));
                enableTrigger(sqlCon,tableName);
            }
        }
        loadDeleteFile(path,sqlCon,file,tableName,"cbMarq","dataBaseSource");
    }
    public static void getDataElement(Connection  sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"EC_No,DataBaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,true,"EC_No"), tableName, path, filename);
        File avroFile = new File(path + "//" + filename);
        if (avroFile.exists())
            executeQuery(sqlCon,updateSelectTable(tableName,true));
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }
}
