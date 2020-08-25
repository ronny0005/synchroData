import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

public class ReglEch extends Table {

    public static String file = "ReglEch.csv";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_REGLECH";
    public static String configList = "listReglEch";

    public static String list()
    {
        return  "SELECT\t[RG_No],[DR_No],[DO_Domaine],[DO_Type]\n" +
                "\t\t,[DO_Piece],[RC_Montant],[RG_TypeReg],[cbProt],[cbMarq],[cbCreateur]\n" +
                "\t\t,[cbModification],[cbReplication],[cbFlag]" +
                ", cbMarqSource = cbMarq\n" +
                "\t\t,[DataBaseSource] = '" + dbSource + "' \n" +
                "FROM\t[F_REGLECH]\n" +
                "WHERE\tcbModification>= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName = 'F_REGLECH'),'1900-01-01')";
    }

    public static String insert()
    {
        return "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                "UPDATE\tF_REGLECH_DEST\n" +
                "SET\t[RC_Montant] = REPLACE([RC_Montant],',','.')\n" +
                "\n" +
                "UPDATE [dbo].[F_REGLECH]\n" +
                "   SET [RC_Montant] = F_REGLECH_DEST.RC_Montant\n" +
                "      ,[RG_TypeReg] = F_REGLECH_DEST.RG_TypeReg\n" +
                "      ,[cbProt] = F_REGLECH_DEST.cbProt\n" +
                "      ,[cbCreateur] = F_REGLECH_DEST.cbCreateur\n" +
                "      ,[cbModification] = F_REGLECH_DEST.cbModification\n" +
                "      ,[cbReplication] = F_REGLECH_DEST.cbReplication\n" +
                "      ,[cbFlag] = F_REGLECH_DEST.cbFlag\n" +
                "FROM F_REGLECH_DEST\n" +
                "WHERE \tF_REGLECH.cbMarqSource = F_REGLECH_DEST.cbMarq\n" +
                "\tAND\tF_REGLECH.DataBaseSource= F_REGLECH_DEST.DataBaseSource\n" +
                "            \n" +
                "INSERT INTO F_REGLECH (\n" +
                "[RG_No],[DR_No],[DO_Domaine],[DO_Type]\n" +
                "\t\t,[DO_Piece],[RC_Montant],[RG_TypeReg],[cbProt],[cbCreateur]\n" +
                "\t\t,[cbModification],[cbReplication],[cbFlag],cbMarqSource,DataBaseSource,DR_NoSource,RG_NoSource)\n" +
                "            \n" +
                "SELECT \tcre.[RG_No],fdr.[DR_No],dest.[DO_Domaine],dest.[DO_Type]\n" +
                "\t\t,dest.[DO_Piece],dest.[RC_Montant],dest.[RG_TypeReg],dest.[cbProt],dest.[cbCreateur]\n" +
                "\t\t,dest.[cbModification],dest.[cbReplication],dest.[cbFlag],dest.cbMarq,dest.DataBaseSource,dest.DR_No,dest.RG_No\n" +
                "FROM F_REGLECH_DEST dest\n" +
                "LEFT JOIN (SELECT DataBaseSource,cbMarqSource FROM F_REGLECH) src\n" +
                "\tON\tdest.DataBaseSource = src.DataBaseSource\n" +
                "\tAND dest.cbMarq = src.cbMarqSource\n" +
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
                "   'F_REGLECH',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {
        dbSource = database;
        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        /*readOnFile(path,file,"F_REGLECH_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"F_REGLECH_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());

         */
        deleteTempTable(sqlCon);
        deleteReglEch(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        /*initTable(sqlCon);
        getData(sqlCon, list(), "F_REGLECH", path, file);
        listDeleteReglEch(sqlCon, path, "deleteList" + file);
         */
    }
    public static void initTable(Connection sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_REGLECH') " +
                " INSERT INTO config.ListReglEch " +
                " SELECT DO_Domaine,DO_Type,DO_Piece,DR_No,RG_No,DataBaseSource = '" + dbSource + "',cbMarq " +
                " FROM F_REGLECH " +
                " ELSE " +
                "    BEGIN " +
                "        INSERT INTO config.ListReglEch" +
                " SELECT DO_Domaine,DO_Type,DO_Piece,DR_No,RG_No,DataBaseSource = '" + dbSource + "',cbMarq " +
                " FROM F_REGLECH " +
                " WHERE cbMarq > (SELECT Max(cbMarq) FROM config.ListReglEch)" +
                "END ";
        executeQuery(sqlCon, query);
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
    public static void listDeleteReglEch(Connection sqlCon, String path, String file)
    {
        String query = " SELECT lart.DO_Domaine,lart.DO_Type,lart.DO_Piece,lart.DataBaseSource,lart.cbMarq " +
                " FROM config.ListReglEch lart " +
                " LEFT JOIN dbo.F_REGLECH fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\" + file, query, sqlCon);

        query = " DELETE FROM config.ListReglEch " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_REGLECH " +
                "                  WHERE dbo.F_REGLECH.cbMarq = config.ListReglEch.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
