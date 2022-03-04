import org.json.simple.JSONObject;

import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;
import java.sql.SQLException;

public class DocLigne extends Table {

    public static String file = "DocLigne_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_DOCLIGNE";
    public static String configList = "listDocLigne";

    public static String insert()
    {
        return  "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +

                "INSERT INTO F_DOCLIGNE (\n" +
                "[DO_Domaine],[DO_Type],[CT_Num],[DO_Piece],[DL_PieceBC],[DL_PieceBL],[DO_Date],[DL_DateBC],[DL_DateBL],[DL_Ligne],[DO_Ref],[DL_TNomencl]\n" +
                "      ,[DL_TRemPied],[DL_TRemExep],[AR_Ref],[DL_Design],[DL_Qte],[DL_QteBC],[DL_QteBL],[DL_PoidsNet] ,[DL_PoidsBrut] \n" +
                "\t  ,[DL_Remise01REM_Valeur] ,[DL_Remise01REM_Type],[DL_Remise02REM_Valeur],[DL_Remise02REM_Type],[DL_Remise03REM_Valeur] ,[DL_Remise03REM_Type]\n" +
                "      ,[DL_PrixUnitaire] ,[DL_PUBC] ,[DL_Taxe1] ,[DL_TypeTaux1],[DL_TypeTaxe1],[DL_Taxe2] ,[DL_TypeTaux2],[DL_TypeTaxe2],[CO_No]/*,[AG_No1],[AG_No2]*/\n" +
                "\t  ,[DL_PrixRU] ,[DL_CMUP] ,[DL_MvtStock],[DT_No],[AF_RefFourniss],[EU_Enumere],[EU_Qte] ,[DL_TTC]\n" +
                "      ,[DE_No],[DL_NoRef],[DL_TypePL],[DL_PUDevise] ,[DL_PUTTC] ,[DL_No],[DO_DateLivr],[CA_Num],[DL_Taxe3] ,[DL_TypeTaux3],[DL_TypeTaxe3],[DL_Frais] \n" +
                "      ,[DL_Valorise],[AR_RefCompose],[DL_NonLivre],[AC_RefClient],[DL_MontantHT] ,[DL_MontantTTC] ,[DL_FactPoids],[DL_Escompte],[DL_PiecePL],[DL_DatePL],[DL_QtePL] ,[DL_NoColis],[DL_NoLink]\n" +
                "      ,[RP_Code],[DL_QteRessource],[DL_DateAvancement],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]" +
                "      ,[MOTIFS_REMISE],[RECEPTION],[USERG],[DATEMODIF],[CONTROLE]" +
                "      ,[NBJ],[NOM_CLIENT],[PMIN],[PMAX],[CONTROLEDATE],[PGROS],cbMarqSource,DataBaseSource)\n" +
                "            \n" +
                "SELECT\t[DO_Domaine],[DO_Type],[CT_Num],[DO_Piece],[DL_PieceBC],[DL_PieceBL],[DO_Date],[DL_DateBC],[DL_DateBL],[DL_Ligne],LEFT(CAST([DO_Ref] AS VARCHAR(150)),17),[DL_TNomencl]\n" +
                "      ,[DL_TRemPied],[DL_TRemExep],[AR_Ref],LEFT([DL_Design],69),[DL_Qte],[DL_QteBC],[DL_QteBL],[DL_PoidsNet] ,[DL_PoidsBrut] \n" +
                "\t  ,[DL_Remise01REM_Valeur] ,[DL_Remise01REM_Type],[DL_Remise02REM_Valeur],[DL_Remise02REM_Type],[DL_Remise03REM_Valeur] ,[DL_Remise03REM_Type]\n" +
                "      ,[DL_PrixUnitaire] ,[DL_PUBC] ,[DL_Taxe1] ,[DL_TypeTaux1],[DL_TypeTaxe1],[DL_Taxe2] ,[DL_TypeTaux2],[DL_TypeTaxe2],[CO_No]/*,[AG_No1],[AG_No2]*/\n" +
                "\t  ,[DL_PrixRU] ,[DL_CMUP] ,[DL_MvtStock],[DT_No],[AF_RefFourniss],[EU_Enumere],[EU_Qte] ,[DL_TTC]\n" +
                "      ,ISNULL(dsrc.[DE_No],dest.DE_No),[DL_NoRef],[DL_TypePL],[DL_PUDevise] ,[DL_PUTTC] ,[DL_No],[DO_DateLivr],[CA_Num],[DL_Taxe3] ,[DL_TypeTaux3],[DL_TypeTaxe3],[DL_Frais] \n" +
                "      ,[DL_Valorise],[AR_RefCompose],[DL_NonLivre],[AC_RefClient],[DL_MontantHT] ,[DL_MontantTTC] ,[DL_FactPoids],[DL_Escompte],[DL_PiecePL],[DL_DatePL],[DL_QtePL] ,[DL_NoColis],[DL_NoLink]\n" +
                "      ,[RP_Code],[DL_QteRessource],[DL_DateAvancement],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]" +
                "      ,[MOTIFS_REMISE],[RECEPTION],[USERG],[DATEMODIF],[CONTROLE]"+
                "      ,[NBJ],[NOM_CLIENT],[PMIN],[PMAX],[CONTROLEDATE],[PGROS],dest.[cbMarqSource],dest.[dataBaseSource]\n" +
                "FROM F_DOCLIGNE_DEST dest\n" +
                "LEFT JOIN (SELECT cbMarqSource,dataBaseSource FROM F_DOCLIGNE) src\n" +
                "\tON\tdest.cbMarqSource = src.cbMarqSource\n" +
                "\tAND\tdest.dataBaseSource = src.dataBaseSource\n" +
                "LEFT JOIN (SELECT DE_NoSource,dataBaseSource,DE_No FROM F_DEPOT) dsrc\n" +
                "\tON\tdsrc.DE_NoSource = dest.DE_No\n" +
                "\tAND\tdsrc.dataBaseSource = dest.dataBaseSource\n" +
                "WHERE src.cbMarqSource IS NULL;\n" +
                "            \n" +
                "IF OBJECT_ID('F_DOCLIGNE_DEST') IS NOT NULL \n" +
                "DROP TABLE F_DOCLIGNE_DEST;\n" +
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
                "   'Insert',\n" +
                "   'F_DOCLIGNE',\n" +
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
                dbSource = database;
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                readOnFile(path, "deleteList" + filename, tableName + "_SUPPR", sqlCon);
                executeQuery(sqlCon, updateTableDest("", "'AG_No1','AG_No2','DL_No','AR_Ref'", tableName, tableName + "_DEST"));
                sendData(sqlCon, path, filename, insert());
                deleteTempTable(sqlCon, tableName);
                deleteDocLigne(sqlCon, path,filename);
            }
        }
    }
    public static void getDataElement(Connection sqlCon, String path, String database, String time, JSONObject type)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,type), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }
    public static void getDataElementFilterAgency(Connection sqlCon, String path,String database,String time,String agency)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DatabaseSource");
        getData(sqlCon, selectSourceTableFilterAgencyEnteteLink(tableName,database,agency), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }
    public static void initTable(Connection sqlCon)
    {
        String query =  " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_DOCLIGNE') " +
                        " INSERT INTO config.ListDocligne " +
                        " SELECT DO_Domaine,DO_Type,DO_Piece,DataBaseSource = '" + dbSource + "',cbMarq " +
                        " FROM F_DOCLIGNE " +
                        " ELSE " +
                        "    BEGIN " +
                        "        INSERT INTO config.ListDocligne" +
                        " SELECT DO_Domaine,DO_Type,DO_Piece,DataBaseSource = '" + dbSource + "',cbMarq " +
                        " FROM F_DOCLIGNE " +
                        " WHERE cbMarq > (SELECT Max(cbMarq) FROM config.ListDocligne)" +
                        "END ";
        executeQuery(sqlCon, query);
    }

    public static void deleteDocLigne(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_DOCLIGNE \n" +
                " WHERE EXISTS (SELECT 1 FROM F_DOCLIGNE_SUPPR WHERE F_DOCLIGNE.cbMarqSource = F_DOCLIGNE_SUPPR.cbMarq AND F_DOCLIGNE.DataBaseSource = F_DOCLIGNE_SUPPR.DataBaseSource) \n" +
                " \n" +
                " IF OBJECT_ID('F_DOCLIGNE_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_DOCLIGNE_SUPPR \n";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }
}
