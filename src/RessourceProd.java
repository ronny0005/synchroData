import java.io.File;
import java.sql.Connection;

public class RessourceProd extends Table {

    public static String file ="ressourceprod.csv";
    public static String tableName = "F_RESSOURCEPROD";
    public static String configList = "listRessourceProd";

    public static String list()
    {
        return "SELECT [RP_Code],[RP_Type],[RP_Intitule],[RP_Complement],[RP_Central] \n "+
                "   ,[RP_Visite],[RP_CoutStd],[RP_Temps],[RP_Sommeil] \n "+
                "   ,[RP_Capacite],[RP_Commentaire],[RP_DateCreation],[RP_TypeRess] \n " +
                "   ,[RP_CodeExterne],[DE_No],[RP_Adresse],[RP_ComplementAdresse],[RP_CodePostal] \n " +
                "   ,[RP_Ville],[RP_Region],[RP_Pays],[RP_Telephone],[RP_Portable] \n " +
                "   ,[RP_EMail],[AR_RefDefaut],[RP_Unite],[RP_Continue],[CAL_No] \n " +
                "   ,[RP_Horaire0101RP_PlageDebut],[RP_Horaire0101RP_PlageFin],[RP_Horaire0102RP_PlageDebut] \n " +
                "   ,[RP_Horaire0102RP_PlageFin],[RP_Horaire0201RP_PlageDebut],[RP_Horaire0201RP_PlageFin],[RP_Horaire0202RP_PlageDebut] \n " +
                "   ,[RP_Horaire0202RP_PlageFin],[RP_Horaire0301RP_PlageDebut],[RP_Horaire0301RP_PlageFin] \n " +
                "   ,[RP_Horaire0302RP_PlageDebut],[RP_Horaire0302RP_PlageFin],[RP_Horaire0401RP_PlageDebut] \n " +
                "   ,[RP_Horaire0401RP_PlageFin],[RP_Horaire0402RP_PlageDebut],[RP_Horaire0402RP_PlageFin] \n " +
                "   ,[RP_Horaire0501RP_PlageDebut],[RP_Horaire0501RP_PlageFin],[RP_Horaire0502RP_PlageDebut] \n " +
                "   ,[RP_Horaire0502RP_PlageFin],[RP_Horaire0601RP_PlageDebut],[RP_Horaire0601RP_PlageFin] \n " +
                "   ,[RP_Horaire0602RP_PlageDebut],[RP_Horaire0602RP_PlageFin],[RP_Horaire0701RP_PlageDebut] \n " +
                "   ,[RP_Horaire0701RP_PlageFin],[RP_Horaire0702RP_PlageDebut],[RP_Horaire0702RP_PlageFin] \n " +
                "   ,[cbProt],[cbMarq],[cbCreateur],[cbModification],[cbReplication],[cbFlag]  \n " +
                " ,cbMarqSource = cbMarq " +
                " FROM\t[F_RESSOURCEPROD] " +
                "WHERE cbModification>= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='F_RESSOURCEPROD'),'1900-01-01')";
    }

    public static String insert()
    {
        return  "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                " UPDATE F_RESSOURCEPROD_DEST    SET[RP_CoutStd] = REPLACE([RP_CoutStd],',','.') \n" +
                "                                \n" +
                "\tUPDATE F_RESSOURCEPROD \n" +
                "\tSET  [RP_Intitule] = F_RESSOURCEPROD_DEST.RP_Intitule\n" +
                "      ,[RP_Complement] = F_RESSOURCEPROD_DEST.RP_Complement\n" +
                "      ,[RP_Central] = F_RESSOURCEPROD_DEST.RP_Central\n" +
                "      ,[RP_Visite] = F_RESSOURCEPROD_DEST.RP_Visite\n" +
                "      ,[RP_CoutStd] = F_RESSOURCEPROD_DEST.RP_CoutStd\n" +
                "      ,[RP_Temps] = F_RESSOURCEPROD_DEST.RP_Temps\n" +
                "      ,[RP_Sommeil] = F_RESSOURCEPROD_DEST.RP_Sommeil\n" +
                "      ,[RP_Capacite] = F_RESSOURCEPROD_DEST.RP_Capacite\n" +
                "      ,[RP_Commentaire] = F_RESSOURCEPROD_DEST.RP_Commentaire\n" +
                "      ,[RP_DateCreation] = F_RESSOURCEPROD_DEST.RP_DateCreation\n" +
                "      ,[RP_CodeExterne] = F_RESSOURCEPROD_DEST.RP_CodeExterne\n" +
                "      ,[DE_No] = F_RESSOURCEPROD_DEST.DE_No\n" +
                "      ,[RP_Adresse] = F_RESSOURCEPROD_DEST.RP_Adresse\n" +
                "      ,[RP_ComplementAdresse] = F_RESSOURCEPROD_DEST.RP_ComplementAdresse\n" +
                "      ,[RP_CodePostal] = F_RESSOURCEPROD_DEST.RP_CodePostal\n" +
                "      ,[RP_Ville] = F_RESSOURCEPROD_DEST.RP_Ville\n" +
                "      ,[RP_Region] = F_RESSOURCEPROD_DEST.RP_Region\n" +
                "      ,[RP_Pays] = F_RESSOURCEPROD_DEST.RP_Pays\n" +
                "      ,[RP_Telephone] = F_RESSOURCEPROD_DEST.RP_Telephone\n" +
                "      ,[RP_Portable] = F_RESSOURCEPROD_DEST.RP_Portable\n" +
                "      ,[RP_EMail] = F_RESSOURCEPROD_DEST.RP_EMail\n" +
                "      ,[AR_RefDefaut] = F_RESSOURCEPROD_DEST.AR_RefDefaut\n" +
                "      ,[RP_Unite] = F_RESSOURCEPROD_DEST.RP_Unite\n" +
                "      ,[RP_Continue] = F_RESSOURCEPROD_DEST.RP_Continue\n" +
                "      ,[CAL_No] = F_RESSOURCEPROD_DEST.CAL_No\n" +
                "      ,[RP_Horaire0101RP_PlageDebut] = F_RESSOURCEPROD_DEST.RP_Horaire0101RP_PlageDebut\n" +
                "      ,[RP_Horaire0101RP_PlageFin] = F_RESSOURCEPROD_DEST.RP_Horaire0101RP_PlageFin\n" +
                "      ,[RP_Horaire0102RP_PlageDebut] = F_RESSOURCEPROD_DEST.RP_Horaire0102RP_PlageDebut\n" +
                "      ,[RP_Horaire0102RP_PlageFin] = F_RESSOURCEPROD_DEST.RP_Horaire0102RP_PlageFin\n" +
                "      ,[RP_Horaire0201RP_PlageDebut] = F_RESSOURCEPROD_DEST.RP_Horaire0201RP_PlageDebut\n" +
                "      ,[RP_Horaire0201RP_PlageFin] = F_RESSOURCEPROD_DEST.RP_Horaire0201RP_PlageFin\n" +
                "      ,[RP_Horaire0202RP_PlageDebut] = F_RESSOURCEPROD_DEST.RP_Horaire0202RP_PlageDebut\n" +
                "      ,[RP_Horaire0202RP_PlageFin] = F_RESSOURCEPROD_DEST.RP_Horaire0202RP_PlageFin\n" +
                "      ,[RP_Horaire0301RP_PlageDebut] = F_RESSOURCEPROD_DEST.RP_Horaire0301RP_PlageDebut\n" +
                "      ,[RP_Horaire0301RP_PlageFin] = F_RESSOURCEPROD_DEST.RP_Horaire0301RP_PlageFin\n" +
                "      ,[RP_Horaire0302RP_PlageDebut] = F_RESSOURCEPROD_DEST.RP_Horaire0302RP_PlageDebut\n" +
                "      ,[RP_Horaire0302RP_PlageFin] = F_RESSOURCEPROD_DEST.RP_Horaire0302RP_PlageFin\n" +
                "      ,[RP_Horaire0401RP_PlageDebut] = F_RESSOURCEPROD_DEST.RP_Horaire0401RP_PlageDebut\n" +
                "      ,[RP_Horaire0401RP_PlageFin] = F_RESSOURCEPROD_DEST.RP_Horaire0401RP_PlageFin\n" +
                "      ,[RP_Horaire0402RP_PlageDebut] = F_RESSOURCEPROD_DEST.RP_Horaire0402RP_PlageDebut\n" +
                "      ,[RP_Horaire0402RP_PlageFin] = F_RESSOURCEPROD_DEST.RP_Horaire0402RP_PlageFin\n" +
                "      ,[RP_Horaire0501RP_PlageDebut] = F_RESSOURCEPROD_DEST.RP_Horaire0501RP_PlageDebut\n" +
                "      ,[RP_Horaire0501RP_PlageFin] = F_RESSOURCEPROD_DEST.RP_Horaire0501RP_PlageFin\n" +
                "      ,[RP_Horaire0502RP_PlageDebut] = F_RESSOURCEPROD_DEST.RP_Horaire0502RP_PlageDebut\n" +
                "      ,[RP_Horaire0502RP_PlageFin] = F_RESSOURCEPROD_DEST.RP_Horaire0502RP_PlageFin\n" +
                "      ,[RP_Horaire0601RP_PlageDebut] = F_RESSOURCEPROD_DEST.RP_Horaire0601RP_PlageDebut\n" +
                "      ,[RP_Horaire0601RP_PlageFin] = F_RESSOURCEPROD_DEST.RP_Horaire0601RP_PlageFin\n" +
                "      ,[RP_Horaire0602RP_PlageDebut] = F_RESSOURCEPROD_DEST.RP_Horaire0602RP_PlageDebut\n" +
                "      ,[RP_Horaire0602RP_PlageFin] = F_RESSOURCEPROD_DEST.RP_Horaire0602RP_PlageFin\n" +
                "      ,[RP_Horaire0701RP_PlageDebut] = F_RESSOURCEPROD_DEST.RP_Horaire0701RP_PlageDebut\n" +
                "      ,[RP_Horaire0701RP_PlageFin] = F_RESSOURCEPROD_DEST.RP_Horaire0701RP_PlageFin\n" +
                "      ,[RP_Horaire0702RP_PlageDebut] = F_RESSOURCEPROD_DEST.RP_Horaire0702RP_PlageDebut\n" +
                "      ,[RP_Horaire0702RP_PlageFin] = F_RESSOURCEPROD_DEST.RP_Horaire0702RP_PlageFin\n" +
                "      ,[cbProt] = F_RESSOURCEPROD_DEST.cbProt\n" +
                "      ,[cbCreateur] = F_RESSOURCEPROD_DEST.cbCreateur\n" +
                "      ,[cbModification] = F_RESSOURCEPROD_DEST.cbModification\n" +
                "      ,[cbReplication] = F_RESSOURCEPROD_DEST.cbReplication\n" +
                "      ,[cbFlag] = F_RESSOURCEPROD_DEST.cbFlag\n" +
                "\tFROM F_RESSOURCEPROD_DEST\n" +
                "\tWHERE F_RESSOURCEPROD.RP_Code = F_RESSOURCEPROD_DEST.RP_Code\n" +
                "                                \n" +
                "                                \n" +
                "\tINSERT INTO F_RESSOURCEPROD (\n" +
                "\t[RP_Code],[RP_Type],[RP_Intitule],[RP_Complement],[RP_Central]\n" +
                "      ,[RP_Visite],[RP_CoutStd],[RP_Temps],[RP_Sommeil]\n" +
                "      ,[RP_Capacite],[RP_Commentaire],[RP_DateCreation],[RP_TypeRess]\n" +
                "      ,[RP_CodeExterne],[DE_No],[RP_Adresse],[RP_ComplementAdresse],[RP_CodePostal]\n" +
                "      ,[RP_Ville],[RP_Region],[RP_Pays],[RP_Telephone],[RP_Portable]\n" +
                "      ,[RP_EMail],[AR_RefDefaut],[RP_Unite],[RP_Continue],[CAL_No]\n" +
                "      ,[RP_Horaire0101RP_PlageDebut],[RP_Horaire0101RP_PlageFin],[RP_Horaire0102RP_PlageDebut]\n" +
                "      ,[RP_Horaire0102RP_PlageFin],[RP_Horaire0201RP_PlageDebut],[RP_Horaire0201RP_PlageFin],[RP_Horaire0202RP_PlageDebut]\n" +
                "      ,[RP_Horaire0202RP_PlageFin],[RP_Horaire0301RP_PlageDebut],[RP_Horaire0301RP_PlageFin]\n" +
                "      ,[RP_Horaire0302RP_PlageDebut],[RP_Horaire0302RP_PlageFin],[RP_Horaire0401RP_PlageDebut]\n" +
                "      ,[RP_Horaire0401RP_PlageFin],[RP_Horaire0402RP_PlageDebut],[RP_Horaire0402RP_PlageFin]\n" +
                "      ,[RP_Horaire0501RP_PlageDebut],[RP_Horaire0501RP_PlageFin],[RP_Horaire0502RP_PlageDebut]\n" +
                "      ,[RP_Horaire0502RP_PlageFin],[RP_Horaire0601RP_PlageDebut],[RP_Horaire0601RP_PlageFin]\n" +
                "      ,[RP_Horaire0602RP_PlageDebut],[RP_Horaire0602RP_PlageFin],[RP_Horaire0701RP_PlageDebut]\n" +
                "      ,[RP_Horaire0701RP_PlageFin],[RP_Horaire0702RP_PlageDebut],[RP_Horaire0702RP_PlageFin]\n" +
                "      ,[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag])\n" +
                "                                \n" +
                "\tSELECT dest.[RP_Code],[RP_Type],[RP_Intitule],[RP_Complement],[RP_Central]\n" +
                "      ,[RP_Visite],[RP_CoutStd],[RP_Temps],[RP_Sommeil]\n" +
                "      ,[RP_Capacite],[RP_Commentaire],[RP_DateCreation],[RP_TypeRess]\n" +
                "      ,[RP_CodeExterne],[DE_No],[RP_Adresse],[RP_ComplementAdresse],[RP_CodePostal]\n" +
                "      ,[RP_Ville],[RP_Region],[RP_Pays],[RP_Telephone],[RP_Portable]\n" +
                "      ,[RP_EMail],null/*[AR_RefDefaut]*/,[RP_Unite],[RP_Continue],[CAL_No]\n" +
                "      ,[RP_Horaire0101RP_PlageDebut],[RP_Horaire0101RP_PlageFin],[RP_Horaire0102RP_PlageDebut]\n" +
                "      ,[RP_Horaire0102RP_PlageFin],[RP_Horaire0201RP_PlageDebut],[RP_Horaire0201RP_PlageFin],[RP_Horaire0202RP_PlageDebut]\n" +
                "      ,[RP_Horaire0202RP_PlageFin],[RP_Horaire0301RP_PlageDebut],[RP_Horaire0301RP_PlageFin]\n" +
                "      ,[RP_Horaire0302RP_PlageDebut],[RP_Horaire0302RP_PlageFin],[RP_Horaire0401RP_PlageDebut]\n" +
                "      ,[RP_Horaire0401RP_PlageFin],[RP_Horaire0402RP_PlageDebut],[RP_Horaire0402RP_PlageFin]\n" +
                "      ,[RP_Horaire0501RP_PlageDebut],[RP_Horaire0501RP_PlageFin],[RP_Horaire0502RP_PlageDebut]\n" +
                "      ,[RP_Horaire0502RP_PlageFin],[RP_Horaire0601RP_PlageDebut],[RP_Horaire0601RP_PlageFin]\n" +
                "      ,[RP_Horaire0602RP_PlageDebut],[RP_Horaire0602RP_PlageFin],[RP_Horaire0701RP_PlageDebut]\n" +
                "      ,[RP_Horaire0701RP_PlageFin],[RP_Horaire0702RP_PlageDebut],[RP_Horaire0702RP_PlageFin]\n" +
                "      ,[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]\n" +
                "\t FROM F_RESSOURCEPROD_DEST dest\n" +
                "\tLEFT JOIN (SELECT RP_Code FROM F_RESSOURCEPROD) src\n" +
                "\t\tON dest.RP_Code = src.RP_Code\n" +
                "\tWHERE src.RP_Code IS NULL\n" +
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
                "   'F_RESSOURCEPROD',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }

    public static void sendDataElement(Connection sqlCon, String path,String database)
    {
        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        /*readOnFile(path,file,"F_RESSOURCEPROD_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"F_RESSOURCEPROD_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());

         */
        deleteRessourceProd(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String database)
    {
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        /*initTable(sqlCon);
        getData(sqlCon, list(), "F_RESSOURCEPROD", path, file);
        listDeleteRessourceProd(sqlCon, path);

         */

    }
    public static void initTable(Connection sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_RESSOURCEPROD') " +
                " INSERT INTO config.ListRessourceProd " +
                " SELECT RP_Code,RP_Type,cbMarq " +
                " FROM F_RESSOURCEPROD ";
        executeQuery(sqlCon, query);
    }
    public static void deleteRessourceProd(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_RESSOURCEPROD \n" +
                " WHERE EXISTS (SELECT 1 FROM F_RESSOURCEPROD_SUPPR WHERE F_RESSOURCEPROD_SUPPR.RP_Type = F_RESSOURCEPROD.RP_Type AND F_RESSOURCEPROD_SUPPR.RP_Code = F_RESSOURCEPROD.RP_Code) \n" +
                " \n" +
                " IF OBJECT_ID('F_RESSOURCEPROD_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_RESSOURCEPROD_SUPPR \n";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }


    public static void listDeleteRessourceProd(Connection sqlCon, String path)
    {
        String query = " SELECT lart.RP_Code,lart.RP_Type,lart.cbMarq " +
                " FROM config.ListRessourceProd lart " +
                " LEFT JOIN dbo.F_RESSOURCEPROD fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\deleteList" + file, query, sqlCon);

        query = " DELETE FROM config.ListRessourceProd " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_RESSOURCEPROD " +
                "                  WHERE dbo.F_RESSOURCEPROD.cbMarq = config.ListRessourceProd.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
