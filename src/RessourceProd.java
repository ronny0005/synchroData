import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class RessourceProd extends Table {

    public static String file ="ressourceprod_";
    public static String tableName = "F_RESSOURCEPROD";
    public static String configList = "listRessourceProd";

    public static String insert(String filename)
    {
        return  "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +

                " IF OBJECT_ID('F_RESSOURCEPROD_DEST') IS NOT NULL\n"+
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
                "   'Insert '+ ' "+filename+"',\n" +
                "   'F_RESSOURCEPROD',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }

    public static void sendDataElement(Connection sqlCon, String path,int unibase)
    {
        File dir = new File(path);
        FilenameFilter filter = (dir1, name) -> name.startsWith(file);
        String[] children = dir.list(filter);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                readOnFile(path, "deleteList" + filename, tableName + "_SUPPR", sqlCon);
                executeQuery(sqlCon, updateTableDest("RP_Code,RP_Type", "'RP_Code','RP_Type','RP_TypeRess'", tableName, tableName + "_DEST", filename,unibase));
                sendData(sqlCon, path, filename, insert(filename));

                deleteTempTable(sqlCon, tableName + "_DEST");
                deleteRessourceProd(sqlCon, path, filename);
            }
        }
    }
    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"RP_Code,RP_Type");
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

    public static void deleteRessourceProd(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_RESSOURCEPROD \n" +
                " WHERE EXISTS (SELECT 1 FROM F_RESSOURCEPROD_SUPPR WHERE F_RESSOURCEPROD_SUPPR.RP_Type = F_RESSOURCEPROD.RP_Type AND F_RESSOURCEPROD_SUPPR.RP_Code = F_RESSOURCEPROD.RP_Code) \n" +
                " \n" +
                " IF OBJECT_ID('F_RESSOURCEPROD_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_RESSOURCEPROD_SUPPR \n";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }
}
