import java.io.File;
import java.sql.Connection;

public class Collaborateur extends Table {

    public static String file = "collaborateur.csv";
    public static String tableName = "F_COLLABORATEUR";
    public static String configList = "listCollaborateur";
    public static String list()
    {
        return "SELECT[CO_No],[CO_Nom],[CO_Prenom],[CO_Fonction],[CO_Adresse],[CO_Complement],[CO_CodePostal],[CO_Ville],[CO_CodeRegion]\n" +
                "      ,[CO_Pays],[CO_Service],[CO_Vendeur],[CO_Caissier],[CO_DateCreation],[CO_Acheteur],[CO_Telephone],[CO_Telecopie]\n" +
                "      ,[CO_EMail],[CO_Receptionnaire],[PROT_No],[CO_TelPortable],[CO_ChargeRecouvr],[cbProt],[cbMarq],[cbCreateur]\n" +
                "      ,[cbModification],[cbReplication],[cbFlag], cbMarqSource = cbMarq\n" +
                "  FROM [dbo].[F_COLLABORATEUR]\n" +
                "  WHERE cbModification>= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName = 'F_COLLABORATEUR'),'1900-01-01')";
    }

    public static String insert()
    {
        return  "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                " UPDATE F_COLLABORATEUR \n" +
                "SET  [CO_Nom] = F_COLLABORATEUR_DEST.CO_Nom\n" +
                "      ,[CO_Prenom] = F_COLLABORATEUR_DEST.CO_Prenom\n" +
                "      ,[CO_Fonction] = F_COLLABORATEUR_DEST.CO_Fonction\n" +
                "      ,[CO_Adresse] = F_COLLABORATEUR_DEST.CO_Adresse\n" +
                "      ,[CO_Complement] = F_COLLABORATEUR_DEST.CO_Complement\n" +
                "      ,[CO_CodePostal] = F_COLLABORATEUR_DEST.CO_CodePostal\n" +
                "      ,[CO_Ville] = F_COLLABORATEUR_DEST.CO_Ville\n" +
                "      ,[CO_CodeRegion] = F_COLLABORATEUR_DEST.CO_CodeRegion\n" +
                "      ,[CO_Pays] = F_COLLABORATEUR_DEST.CO_Pays\n" +
                "      ,[CO_Service] = F_COLLABORATEUR_DEST.CO_Service\n" +
                "      ,[CO_Vendeur] = F_COLLABORATEUR_DEST.CO_Vendeur\n" +
                "      ,[CO_Caissier] = F_COLLABORATEUR_DEST.CO_Caissier\n" +
                "      ,[CO_DateCreation] = F_COLLABORATEUR_DEST.CO_DateCreation\n" +
                "      ,[CO_Acheteur] = F_COLLABORATEUR_DEST.CO_Acheteur\n" +
                "      ,[CO_Telephone] = F_COLLABORATEUR_DEST.CO_Telephone\n" +
                "      ,[CO_Telecopie] = F_COLLABORATEUR_DEST.CO_Telecopie\n" +
                "      ,[CO_EMail] = F_COLLABORATEUR_DEST.CO_EMail\n" +
                "      ,[CO_Receptionnaire] = F_COLLABORATEUR_DEST.CO_Receptionnaire\n" +
                "      ,[PROT_No] = F_COLLABORATEUR_DEST.PROT_No\n" +
                "      ,[CO_TelPortable] = F_COLLABORATEUR_DEST.CO_TelPortable\n" +
                "      ,[CO_ChargeRecouvr] = F_COLLABORATEUR_DEST.CO_ChargeRecouvr\n" +
                "      ,[cbProt] = F_COLLABORATEUR_DEST.cbProt\n" +
                "      ,[cbCreateur] = F_COLLABORATEUR_DEST.cbCreateur\n" +
                "      ,[cbModification] = F_COLLABORATEUR_DEST.cbModification\n" +
                "      ,[cbReplication] = F_COLLABORATEUR_DEST.cbReplication\n" +
                "      ,[cbFlag] = F_COLLABORATEUR_DEST.cbFlag\n" +
                " FROM F_COLLABORATEUR_DEST\n" +
                " WHERE\tF_COLLABORATEUR.CO_No = F_COLLABORATEUR_DEST.CO_No\n" +
                "            \n" +
                "INSERT INTO F_COLLABORATEUR (\n" +
                "[CO_No],[CO_Nom],[CO_Prenom],[CO_Fonction],[CO_Adresse],[CO_Complement],[CO_CodePostal],[CO_Ville],[CO_CodeRegion]\n" +
                "      ,[CO_Pays],[CO_Service],[CO_Vendeur],[CO_Caissier],[CO_DateCreation],[CO_Acheteur],[CO_Telephone],[CO_Telecopie]\n" +
                "      ,[CO_EMail],[CO_Receptionnaire],[PROT_No],[CO_TelPortable],[CO_ChargeRecouvr],[cbProt],[cbCreateur]\n" +
                "      ,[cbModification],[cbReplication],[cbFlag])\n" +
                "            \n" +
                "SELECT dest.[CO_No],[CO_Nom],[CO_Prenom],[CO_Fonction],[CO_Adresse],[CO_Complement],[CO_CodePostal],[CO_Ville],[CO_CodeRegion]\n" +
                "      ,[CO_Pays],[CO_Service],[CO_Vendeur],[CO_Caissier],[CO_DateCreation],[CO_Acheteur],[CO_Telephone],[CO_Telecopie]\n" +
                "      ,[CO_EMail],[CO_Receptionnaire],[PROT_No],[CO_TelPortable],[CO_ChargeRecouvr],[cbProt],[cbCreateur]\n" +
                "      ,[cbModification],[cbReplication],[cbFlag]\n" +
                "FROM F_COLLABORATEUR_DEST dest\n" +
                "LEFT JOIN (SELECT CO_No FROM F_COLLABORATEUR) src\n" +
                "\tON\tdest.CO_No = src.CO_No\n" +
                "WHERE src.CO_No IS NULL\n" +
                "            \n" +
                "IF OBJECT_ID('F_COLLABORATEUR_DEST') IS NOT NULL \n" +
                "DROP TABLE F_COLLABORATEUR_DEST;\n" +
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
                "   'F_COLLABORATEUR',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }

    public static void deleteTempTable(Connection sqlCon)
    {
        String query = "IF OBJECT_ID('F_COLLABORATEUR_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_COLLABORATEUR_DEST;";
        executeQuery(sqlCon, query);
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        /*readOnFile(path,file,"F_COLLABORATEUR_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"F_COLLABORATEUR_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());

         */
        deleteTempTable(sqlCon);
        deleteCollaborateur(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        /*initTable(sqlCon);
        getData(sqlCon, list(), "F_COLLABORATEUR", path, file);
        listDeleteCollaborateur(sqlCon, path, "deleteList" + file);*/
    }
    public static void initTable(Connection sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_COLLABORATEUR') " +
                " INSERT INTO config.ListCollaborateur " +
                " SELECT CO_No,cbMarq " +
                " FROM F_COLLABORATEUR ";
        executeQuery(sqlCon, query);
    }

    public static void listDeleteCollaborateur(Connection sqlCon, String path, String file)
    {
        String query = " SELECT lart.CO_No,lart.cbMarq " +
                " FROM config.ListCollaborateur lart " +
                " LEFT JOIN dbo.F_COLLABORATEUR fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\" + file, query, sqlCon);

        query = " DELETE FROM config.ListCollaborateur " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_COLLABORATEUR " +
                "                  WHERE dbo.F_COLLABORATEUR.cbMarq = config.ListCollaborateur.cbMarq);";
        executeQuery(sqlCon, query);
    }
    public static void deleteCollaborateur(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_COLLABORATEUR \n" +
                " WHERE CO_No IN(SELECT CO_No FROM F_COLLABORATEUR_SUPPR) \n" +
                " \n" +
                " IF OBJECT_ID('F_COLLABORATEUR_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_COLLABORATEUR_SUPPR \n";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }
}
