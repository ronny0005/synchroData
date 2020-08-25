import java.io.File;
import java.sql.Connection;

public class Livraison extends Table {

    public static String file ="livraison.csv";
    public static String tableName = "F_LIVRAISON";
    public static String configList = "listLivraison";

    public static String list()
    {
        return "SELECT\t[LI_No],[CT_Num],[LI_Intitule],[LI_Adresse],[LI_Complement],[LI_CodePostal],[LI_Ville]\n" +
                "\t\t,[LI_CodeRegion],[LI_Pays],[LI_Contact],[N_Expedition],[N_Condition],[LI_Principal]\n" +
                "\t\t,[LI_Telephone],[LI_Telecopie],[LI_EMail],[cbProt],[cbCreateur]\n" +
                "\t\t,[cbModification],[cbReplication],[cbFlag],cbMarqSource = [cbMarq]\n" +
                "FROM\t[F_LIVRAISON] "+
                "WHERE cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='F_LIVRAISON'),'1900-01-01')";
    }

    public static String insert()
    {
        return  "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                "UPDATE F_LIVRAISON  \n" +
                "SET  [LI_Intitule] = F_LIVRAISON_DEST.LI_Intitule\n" +
                "      ,[LI_Adresse] = F_LIVRAISON_DEST.LI_Adresse\n" +
                "      ,[LI_Complement] = F_LIVRAISON_DEST.LI_Complement\n" +
                "      ,[LI_CodePostal] = F_LIVRAISON_DEST.LI_CodePostal\n" +
                "      ,[LI_Ville] = F_LIVRAISON_DEST.LI_Ville\n" +
                "      ,[LI_CodeRegion] = F_LIVRAISON_DEST.LI_CodeRegion\n" +
                "      ,[LI_Pays] = F_LIVRAISON_DEST.LI_Pays\n" +
                "      ,[LI_Contact] = F_LIVRAISON_DEST.LI_Contact\n" +
                "      ,[N_Expedition] = F_LIVRAISON_DEST.N_Expedition\n" +
                "      ,[N_Condition] = F_LIVRAISON_DEST.N_Condition\n" +
                "      ,[LI_Principal] = F_LIVRAISON_DEST.LI_Principal\n" +
                "      ,[LI_Telephone] = F_LIVRAISON_DEST.LI_Telephone\n" +
                "      ,[LI_Telecopie] = F_LIVRAISON_DEST.LI_Telecopie\n" +
                "      ,[LI_EMail] = F_LIVRAISON_DEST.LI_EMail\n" +
                "      ,[cbProt] = F_LIVRAISON_DEST.cbProt\n" +
                "      ,[cbCreateur] = F_LIVRAISON_DEST.cbCreateur\n" +
                "      ,[cbModification] = F_LIVRAISON_DEST.cbModification\n" +
                "      ,[cbReplication] = F_LIVRAISON_DEST.cbReplication\n" +
                "      ,[cbFlag] = F_LIVRAISON_DEST.cbFlag \n" +
                "FROM F_LIVRAISON_DEST \n" +
                "WHERE F_LIVRAISON.LI_No = F_LIVRAISON_DEST.LI_No\n" +
                "AND F_LIVRAISON.CT_Num = F_LIVRAISON_DEST.CT_Num \n" +
                "                                 \n" +
                "INSERT INTO F_LIVRAISON ( [LI_No],[CT_Num],[LI_Intitule],[LI_Adresse],[LI_Complement],[LI_CodePostal],[LI_Ville]\n" +
                ",[LI_CodeRegion],[LI_Pays],[LI_Contact],[N_Expedition],[N_Condition],[LI_Principal]\n" +
                ",[LI_Telephone],[LI_Telecopie],[LI_EMail],[cbProt],[cbCreateur]\n" +
                ",[cbModification],[cbReplication],[cbFlag]) \n" +
                "                                 \n" +
                "SELECT dest.[LI_No],dest.[CT_Num],[LI_Intitule],[LI_Adresse],[LI_Complement],[LI_CodePostal],[LI_Ville]\n" +
                ",[LI_CodeRegion],[LI_Pays],[LI_Contact],[N_Expedition],[N_Condition],[LI_Principal]\n" +
                ",[LI_Telephone],[LI_Telecopie],[LI_EMail],[cbProt],[cbCreateur]\n" +
                ",[cbModification],[cbReplication],[cbFlag]\n" +
                "FROM F_LIVRAISON_DEST dest \n" +
                "LEFT JOIN (SELECT [LI_No],[CT_Num] FROM F_LIVRAISON) src \n" +
                "ON dest.[LI_No] = src.[LI_No] \n" +
                "AND dest.[CT_Num] = src.[CT_Num] \n" +
                "WHERE src.[LI_No] IS NULL ;" +
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
                "   'F_LIVRAISON',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }

    public static void sendDataElement(Connection sqlCon, String path,String database)
    {

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        /*readOnFile(path,file,"F_LIVRAISON_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"F_LIVRAISON_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());
         */
        deleteLivraison(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        /*initTable(sqlCon);
        getData(sqlCon, list(), "F_LIVRAISON", path, file);
        listDeleteLivraison(sqlCon, path);*/
    }
    public static void initTable(Connection sqlCon)
    {
        String query =  " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_LIVRAISON') \n" +
                "     INSERT INTO config.ListLivraison\n" +
                "     SELECT LI_No,CT_Num,cbMarq \n" +
                "     FROM F_LIVRAISON";
        executeQuery(sqlCon, query);
    }
    public static void deleteLivraison(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_LIVRAISON \n" +
                " WHERE EXISTS (SELECT 1 FROM F_LIVRAISON_SUPPR WHERE F_LIVRAISON_SUPPR.LI_No = F_LIVRAISON.LI_No AND F_LIVRAISON_SUPPR.CT_Num = F_LIVRAISON.CT_Num) \n" +
                " \n" +
                " IF OBJECT_ID('F_LIVRAISON_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_LIVRAISON_SUPPR \n";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }

    public static void listDeleteLivraison(Connection sqlCon, String path)
    {
        String query = " SELECT lart.LI_No,lart.CT_Num,lart.cbMarq " +
                " FROM config.ListLivraison lart " +
                " LEFT JOIN dbo.F_LIVRAISON fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\deleteList" + file, query, sqlCon);

        query = " DELETE FROM config.ListLivraison " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_LIVRAISON " +
                "                  WHERE dbo.F_LIVRAISON.cbMarq = config.ListLivraison.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
