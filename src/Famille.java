import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class Famille extends Table {

    public static String file ="famille_";
    public static String tableName = "F_FAMILLE";
    public static String configList = "listFamille";

    public static String insert(String filename)
    {
        return "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                "INSERT INTO [dbo].[F_FAMILLE]\n" +
                "    ([FA_CodeFamille],[FA_Type],[FA_Intitule],[FA_UniteVen],[FA_Coef],[FA_SuiviStock],[FA_Garantie],[FA_Central]\n" +
                "    ,[FA_Stat01],[FA_Stat02],[FA_Stat03],[FA_Stat04],[FA_Stat05],[FA_CodeFiscal],[FA_Pays],[FA_UnitePoids]\n" +
                "    ,[FA_Escompte],[FA_Delai],[FA_HorsStat],[FA_VteDebit],[FA_NotImp],[FA_Frais01FR_Denomination]\n" +
                "    ,[FA_Frais01FR_Rem01REM_Valeur],[FA_Frais01FR_Rem01REM_Type],[FA_Frais01FR_Rem02REM_Valeur],[FA_Frais01FR_Rem02REM_Type]\n" +
                "    ,[FA_Frais01FR_Rem03REM_Valeur],[FA_Frais01FR_Rem03REM_Type],[FA_Frais02FR_Denomination],[FA_Frais02FR_Rem01REM_Valeur]\n" +
                "    ,[FA_Frais02FR_Rem01REM_Type],[FA_Frais02FR_Rem02REM_Valeur],[FA_Frais02FR_Rem02REM_Type],[FA_Frais02FR_Rem03REM_Valeur]\n" +
                "    ,[FA_Frais02FR_Rem03REM_Type],[FA_Frais03FR_Denomination],[FA_Frais03FR_Rem01REM_Valeur],[FA_Frais03FR_Rem01REM_Type]\n" +
                "    ,[FA_Frais03FR_Rem02REM_Valeur],[FA_Frais03FR_Rem02REM_Type],[FA_Frais03FR_Rem03REM_Valeur],[FA_Frais03FR_Rem03REM_Type]\n" +
                "    ,[FA_Contremarque],[FA_FactPoids],[FA_FactForfait],[FA_Publie],[FA_RacineRef],[FA_RacineCB],[CL_No1],[CL_No2],[CL_No3],[CL_No4]\n" +
                "    ,[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag])\n" +
                "\t\t\t\t\t\t  \n" +
                "SELECT dest.[FA_CodeFamille],[FA_Type],[FA_Intitule],[FA_UniteVen],[FA_Coef],[FA_SuiviStock],[FA_Garantie],[FA_Central]\n" +
                "           ,[FA_Stat01],[FA_Stat02],[FA_Stat03],[FA_Stat04],[FA_Stat05],[FA_CodeFiscal],[FA_Pays],[FA_UnitePoids]\n" +
                "           ,[FA_Escompte],[FA_Delai],[FA_HorsStat],[FA_VteDebit],[FA_NotImp],[FA_Frais01FR_Denomination]\n" +
                "           ,[FA_Frais01FR_Rem01REM_Valeur],[FA_Frais01FR_Rem01REM_Type],[FA_Frais01FR_Rem02REM_Valeur],[FA_Frais01FR_Rem02REM_Type]\n" +
                "           ,[FA_Frais01FR_Rem03REM_Valeur],[FA_Frais01FR_Rem03REM_Type],[FA_Frais02FR_Denomination],[FA_Frais02FR_Rem01REM_Valeur]\n" +
                "           ,[FA_Frais02FR_Rem01REM_Type],[FA_Frais02FR_Rem02REM_Valeur],[FA_Frais02FR_Rem02REM_Type],[FA_Frais02FR_Rem03REM_Valeur]\n" +
                "           ,[FA_Frais02FR_Rem03REM_Type],[FA_Frais03FR_Denomination],[FA_Frais03FR_Rem01REM_Valeur],[FA_Frais03FR_Rem01REM_Type]\n" +
                "           ,[FA_Frais03FR_Rem02REM_Valeur],[FA_Frais03FR_Rem02REM_Type],[FA_Frais03FR_Rem03REM_Valeur],[FA_Frais03FR_Rem03REM_Type]\n" +
                "           ,[FA_Contremarque],[FA_FactPoids],[FA_FactForfait],[FA_Publie],[FA_RacineRef],[FA_RacineCB],[CL_No1],[CL_No2],[CL_No3],[CL_No4]\n" +
                "           ,[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]\n" +
                "FROM F_FAMILLE_DEST dest  \n" +
                "LEFT JOIN (SELECT [FA_CodeFamille] FROM F_FAMILLE) src  \n" +
                "ON dest.[FA_CodeFamille] = src.[FA_CodeFamille]  \n" +
                "WHERE src.[FA_CodeFamille] IS NULL ;\n" +
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
                "   'F_FAMILLE',\n" +
                "   GETDATE());\n" +
                "END CATCH";

    }

    public static void sendDataElement(Connection sqlCon, String path,String database)
    {
        File dir = new File(path);
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith(file);
            }
        };
        String[] children = dir.list(filter);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (int i = 0; i < children.length; i++) {
                String filename = children[i];
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                readOnFile(path, "deleteList" + filename, tableName + "_SUPPR", sqlCon);
                executeQuery(sqlCon, updateTableDest("FA_CodeFamille", "'FA_CodeFamille','FA_Type'", tableName, tableName + "_DEST",filename));
                sendData(sqlCon, path, filename, insert(filename));

                FamCompta.sendDataElement(sqlCon, path, database);
                deleteTempTable(sqlCon, tableName);
                deleteFamille(sqlCon, path,filename);
            }
        }
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"FA_CodeFamille");
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, filename);

        FamCompta.getDataElement(sqlCon, path,database, time);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);


    }
    public static void deleteFamille(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_FAMILLE  \n" +
                " WHERE EXISTS (SELECT 1 FROM F_FAMILLE_SUPPR WHERE F_FAMILLE_SUPPR.FA_CodeFamille = F_FAMILLE.FA_CodeFamille)  \n" +
                "  \n" +
                " IF OBJECT_ID('F_FAMILLE_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_FAMILLE_SUPPR ;";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }

}
