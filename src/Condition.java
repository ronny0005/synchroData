import java.io.File;
import java.sql.Connection;

public class Condition extends Table{

    public static String file = "condition.csv";
    public static String tableName = "F_CONDITION";
    public static String configList = "listCondition";
    public static String list()
    {
        return "SELECT [AR_Ref],[CO_No],[EC_Enumere],[EC_Quantite]\n " +
                "       ,[CO_Ref],[CO_CodeBarre],[CO_Principal],[cbProt],[cbMarq],[cbCreateur]\n " +
                "       ,[cbModification],[cbReplication],[cbFlag]" +
                ", cbMarqSource = cbMarq\n " +
                " FROM [F_CONDITION]\n " +
                "WHERE cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='F_CONDITION'),'1900-01-01')";
    }

    public static String insert()
    {
        return  " BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                " UPDATE F_CONDITION_DEST " +
                "   SET[EC_Quantite] = REPLACE([EC_Quantite],',','.') \n" +
                "                                \n" +
                "\tUPDATE F_CONDITION \n" +
                "\tSET  [CO_Ref] = F_CONDITION_DEST.CO_Ref\n" +
                "\t\t  ,[CO_CodeBarre] = F_CONDITION_DEST.CO_CodeBarre\n" +
                "\t\t  ,[CO_Principal] = F_CONDITION_DEST.CO_Principal\n" +
                "\t\t  ,[cbProt] = F_CONDITION_DEST.cbProt\n" +
                "\t\t  ,[cbCreateur] = F_CONDITION_DEST.cbCreateur\n" +
                "\t\t  ,[cbModification] = F_CONDITION_DEST.cbModification\n" +
                "\t\t  ,[cbReplication] = F_CONDITION_DEST.cbReplication\n" +
                "\t\t  ,[cbFlag] = F_CONDITION_DEST.cbFlag\n" +
                "\tFROM F_CONDITION_DEST\n" +
                "\tWHERE F_CONDITION.CO_No = F_CONDITION_DEST.CO_No\n" +
                "                                \n" +
                "\tINSERT INTO F_CONDITION (\n" +
                "\t[AR_Ref],[CO_No],[EC_Enumere],[EC_Quantite]\n" +
                "\t\t\t,[CO_Ref],[CO_CodeBarre],[CO_Principal],[cbProt],[cbCreateur]\n" +
                "\t\t\t,[cbModification],[cbReplication],[cbFlag])\n" +
                "                                \n" +
                "\tSELECT dest.[AR_Ref],dest.[CO_No],dest.[EC_Enumere],[EC_Quantite]\n" +
                "\t\t\t,[CO_Ref],[CO_CodeBarre],[CO_Principal],[cbProt],[cbCreateur]\n" +
                "\t\t\t,[cbModification],[cbReplication],[cbFlag]\n" +
                "\tFROM F_CONDITION_DEST dest\n" +
                "\tLEFT JOIN (SELECT AR_Ref,CO_No,EC_Enumere FROM F_CONDITION) src\n" +
                "\t\tON dest.CO_No = src.CO_No\n" +
                "\t\tAND dest.EC_Enumere = src.EC_Enumere\n" +
                "\t\tAND dest.AR_Ref = src.AR_Ref\n" +
                "\tWHERE src.CO_No IS NULL\n" +
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
                "   'F_CONDITION',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        /*readOnFile(path,file,"F_CONDITION_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"F_CONDITION_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());

         */
        deleteCondition(sqlCon,path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        /*initTable(sqlCon);
        getData(sqlCon, list(), "F_CONDITION", path, file);
        listDeleteCondition(sqlCon, path);*/
    }

    public static void initTable(Connection sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_CONDITION') " +
                " INSERT INTO config.ListCondition " +
                " SELECT CO_No,AR_Ref,cbMarq " +
                " FROM F_CONDITION ";
        executeQuery(sqlCon, query);
    }
    public static void deleteCondition(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_CONDITION \n" +
                " WHERE CO_No IN(SELECT CO_No FROM F_CONDITION_SUPPR) \n" +
                " \n" +
                " IF OBJECT_ID('F_CONDITION_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_CONDITION_SUPPR \n";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }
    public static void listDeleteCondition(Connection sqlCon, String path)
    {
        String query = " SELECT lart.CO_No,lart.AR_Ref,lart.cbMarq " +
                " FROM config.ListCondition lart " +
                " LEFT JOIN dbo.F_CONDITION fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\deleteList" + file, query, sqlCon);

        query = " DELETE FROM config.ListCondition " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_CONDITION " +
                "                  WHERE dbo.F_CONDITION.cbMarq = config.ListCondition.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
