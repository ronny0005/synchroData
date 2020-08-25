import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

public class DocLigne extends Table {

    public static String file = "DocLigne.csv";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_DOCLIGNE";
    public static String configList = "listDocLigne";

    public static String list()
    {
        return "SELECT\t[DO_Domaine],[DO_Type],[CT_Num],[DO_Piece],[DL_PieceBC],[DL_PieceBL]\n" +
                "      ,[DO_Date],[DL_DateBC],[DL_DateBL],[DL_Ligne],[DO_Ref],[DL_TNomencl]\n" +
                "      ,[DL_TRemPied],[DL_TRemExep],[AR_Ref],[DL_Design],[DL_Qte],[DL_QteBC]\n" +
                "      ,[DL_QteBL],[DL_PoidsNet],[DL_PoidsBrut],[DL_Remise01REM_Valeur],[DL_Remise01REM_Type],[DL_Remise02REM_Valeur]\n" +
                "      ,[DL_Remise02REM_Type],[DL_Remise03REM_Valeur],[DL_Remise03REM_Type]\n" +
                "      ,[DL_PrixUnitaire],[DL_PUBC],[DL_Taxe1],[DL_TypeTaux1],[DL_TypeTaxe1],[DL_Taxe2]\n" +
                "      ,[DL_TypeTaux2],[DL_TypeTaxe2],[CO_No],[AG_No1],[AG_No2],[DL_PrixRU],[DL_CMUP]\n" +
                "      ,[DL_MvtStock],[DT_No],[AF_RefFourniss],[EU_Enumere],[EU_Qte],[DL_TTC]\n" +
                "      ,[DE_No],[DL_NoRef],[DL_TypePL],[DL_PUDevise],[DL_PUTTC],[DL_No]\n" +
                "      ,[DO_DateLivr],[CA_Num],[DL_Taxe3],[DL_TypeTaux3],[DL_TypeTaxe3],[DL_Frais]\n" +
                "      ,[DL_Valorise],[AR_RefCompose],[DL_NonLivre],[AC_RefClient],[DL_MontantHT],[DL_MontantTTC]\n" +
                "      ,[DL_FactPoids],[DL_Escompte],[DL_PiecePL],[DL_DatePL],[DL_QtePL],[DL_NoColis],[DL_NoLink]\n" +
                "      ,[RP_Code],[DL_QteRessource],[DL_DateAvancement],[cbProt],[cbMarq],[cbCreateur],[cbModification]\n" +
                "      ,[cbReplication],[cbFlag]\n" +
                "      ,[MOTIFS_REMISE],[RECEPTION],[USERG],[DATEMODIF],[CONTROLE]\n"+
                "      ,[NBJ],[NOM_CLIENT],[PMIN],[PMAX],[CONTROLEDATE],[PGROS]\n" +
                ", cbMarqSource = cbMarq" +
                "\t\t,[DataBaseSource] = '" + dbSource +"' \n" +
                "FROM\t[F_DOCLIGNE]\n" +
                "WHERE\tcbModification>= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName = 'F_DOCLIGNE'),'1900-01-01')";
    }

    public static String insert()
    {
        return  "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                "UPDATE [dbo].[F_DOCLIGNE]\n" +
                "   SET [DO_Domaine] = F_DOCLIGNE_DEST.DO_Domaine\n" +
                "      ,[DO_Type] = F_DOCLIGNE_DEST.DO_Type\n" +
                "      ,[CT_Num] = F_DOCLIGNE_DEST.CT_Num\n" +
                "      ,[DO_Piece] = F_DOCLIGNE_DEST.DO_Piece\n" +
                "      ,[DL_PieceBC] = F_DOCLIGNE_DEST.DL_PieceBC\n" +
                "      ,[DL_PieceBL] = F_DOCLIGNE_DEST.DL_PieceBL\n" +
                "      ,[DO_Date] = F_DOCLIGNE_DEST.DO_Date\n" +
                "      ,[DL_DateBC] = F_DOCLIGNE_DEST.DL_DateBC\n" +
                "      ,[DL_DateBL] = F_DOCLIGNE_DEST.DL_DateBL\n" +
                "      ,[DL_Ligne] = F_DOCLIGNE_DEST.DL_Ligne\n" +
                "      ,[DO_Ref] = LEFT(CAST(F_DOCLIGNE_DEST.DO_Ref AS VARCHAR(150)),17)\n" +
                "      ,[DL_TNomencl] = F_DOCLIGNE_DEST.DL_TNomencl\n" +
                "      ,[DL_TRemPied] = F_DOCLIGNE_DEST.DL_TRemPied\n" +
                "      ,[DL_TRemExep] = F_DOCLIGNE_DEST.DL_TRemExep\n" +
                "      ,[DL_Design] = LEFT(F_DOCLIGNE_DEST.DL_Design,69)\n" +
                "      ,[DL_Qte] = F_DOCLIGNE_DEST.DL_Qte\n" +
                "      ,[DL_QteBC] = F_DOCLIGNE_DEST.DL_QteBC\n" +
                "      ,[DL_QteBL] = F_DOCLIGNE_DEST.DL_QteBL\n" +
                "      ,[DL_PoidsNet] = F_DOCLIGNE_DEST.DL_PoidsNet\n" +
                "      ,[DL_PoidsBrut] = F_DOCLIGNE_DEST.DL_PoidsBrut\n" +
                "      ,[DL_Remise01REM_Valeur] = F_DOCLIGNE_DEST.DL_Remise01REM_Valeur\n" +
                "      ,[DL_Remise01REM_Type] = F_DOCLIGNE_DEST.DL_Remise01REM_Type\n" +
                "      ,[DL_Remise02REM_Valeur] = F_DOCLIGNE_DEST.DL_Remise02REM_Valeur\n" +
                "      ,[DL_Remise02REM_Type] = F_DOCLIGNE_DEST.DL_Remise02REM_Type\n" +
                "      ,[DL_Remise03REM_Valeur] = F_DOCLIGNE_DEST.DL_Remise03REM_Valeur\n" +
                "      ,[DL_Remise03REM_Type] = F_DOCLIGNE_DEST.DL_Remise03REM_Type\n" +
                "      ,[DL_PrixUnitaire] = F_DOCLIGNE_DEST.DL_PrixUnitaire\n" +
                "      ,[DL_PUBC] = F_DOCLIGNE_DEST.DL_PUBC\n" +
                "      ,[DL_Taxe1] = F_DOCLIGNE_DEST.DL_Taxe1\n" +
                "      ,[DL_TypeTaux1] = F_DOCLIGNE_DEST.DL_TypeTaux1\n" +
                "      ,[DL_TypeTaxe1] = F_DOCLIGNE_DEST.DL_TypeTaxe1\n" +
                "      ,[DL_Taxe2] = F_DOCLIGNE_DEST.DL_Taxe2\n" +
                "      ,[DL_TypeTaux2] = F_DOCLIGNE_DEST.DL_TypeTaux2\n" +
                "      ,[DL_TypeTaxe2] = F_DOCLIGNE_DEST.DL_TypeTaxe2\n" +
                "      ,[CO_No] = F_DOCLIGNE_DEST.CO_No\n" +
                //"      ,[AG_No1] = F_DOCLIGNE_DEST.AG_No1\n" +
                //"      ,[AG_No2] = F_DOCLIGNE_DEST.AG_No2\n" +
                "      ,[DL_PrixRU] = F_DOCLIGNE_DEST.DL_PrixRU\n" +
                "      ,[DL_CMUP] = F_DOCLIGNE_DEST.DL_CMUP\n" +
                "      ,[DL_MvtStock] = F_DOCLIGNE_DEST.DL_MvtStock\n" +
                "      ,[DT_No] = F_DOCLIGNE_DEST.DT_No\n" +
                "      ,[AF_RefFourniss] = F_DOCLIGNE_DEST.AF_RefFourniss\n" +
                "      ,[EU_Enumere] = F_DOCLIGNE_DEST.EU_Enumere\n" +
                "      ,[EU_Qte] = F_DOCLIGNE_DEST.EU_Qte\n" +
                "      ,[DL_TTC] = F_DOCLIGNE_DEST.DL_TTC\n" +
                "      ,[DE_No] = F_DOCLIGNE_DEST.DE_No\n" +
                "      ,[DL_NoRef] = F_DOCLIGNE_DEST.DL_NoRef\n" +
                "      ,[DL_TypePL] = F_DOCLIGNE_DEST.DL_TypePL\n" +
                "      ,[DL_PUDevise] = F_DOCLIGNE_DEST.DL_PUDevise\n" +
                "      ,[DL_PUTTC] = F_DOCLIGNE_DEST.DL_PUTTC\n" +
                //"      ,[DL_No] = F_DOCLIGNE_DEST.DL_No\n" +
                "      ,[DO_DateLivr] = F_DOCLIGNE_DEST.DO_DateLivr\n" +
                "      ,[CA_Num] = F_DOCLIGNE_DEST.CA_Num\n" +
                "      ,[DL_Taxe3] = F_DOCLIGNE_DEST.DL_Taxe3\n" +
                "      ,[DL_TypeTaux3] = F_DOCLIGNE_DEST.DL_TypeTaux3\n" +
                "      ,[DL_TypeTaxe3] = F_DOCLIGNE_DEST.DL_TypeTaxe3\n" +
                "      ,[DL_Frais] = F_DOCLIGNE_DEST.DL_Frais\n" +
                "      ,[DL_Valorise] = F_DOCLIGNE_DEST.DL_Valorise\n" +
                "      ,[AR_RefCompose] = F_DOCLIGNE_DEST.AR_RefCompose\n" +
                "      ,[DL_NonLivre] = F_DOCLIGNE_DEST.DL_NonLivre\n" +
                "      ,[AC_RefClient] = F_DOCLIGNE_DEST.AC_RefClient\n" +
                "      ,[DL_MontantHT] = F_DOCLIGNE_DEST.DL_MontantHT\n" +
                "      ,[DL_MontantTTC] = F_DOCLIGNE_DEST.DL_MontantTTC\n" +
                "      ,[DL_FactPoids] = F_DOCLIGNE_DEST.DL_FactPoids\n" +
                "      ,[DL_Escompte] = F_DOCLIGNE_DEST.DL_Escompte\n" +
                "      ,[DL_PiecePL] = F_DOCLIGNE_DEST.DL_PiecePL\n" +
                "      ,[DL_DatePL] = F_DOCLIGNE_DEST.DL_DatePL\n" +
                "      ,[DL_QtePL] = F_DOCLIGNE_DEST.DL_QtePL\n" +
                "      ,[DL_NoColis] = F_DOCLIGNE_DEST.DL_NoColis\n" +
                "      ,[DL_NoLink] = F_DOCLIGNE_DEST.DL_NoLink\n" +
                "      ,[RP_Code] = F_DOCLIGNE_DEST.RP_Code\n" +
                "      ,[DL_QteRessource] = F_DOCLIGNE_DEST.DL_QteRessource\n" +
                "      ,[DL_DateAvancement] = F_DOCLIGNE_DEST.DL_DateAvancement\n" +
                "      ,[cbProt] = F_DOCLIGNE_DEST.cbProt\n" +
                "      ,[cbCreateur] = F_DOCLIGNE_DEST.cbCreateur\n" +
                "      ,[cbModification] = F_DOCLIGNE_DEST.cbModification\n" +
                "      ,[cbReplication] = F_DOCLIGNE_DEST.cbReplication\n" +
                "      ,[cbFlag] = F_DOCLIGNE_DEST.cbFlag\n" +
                "      ,[MOTIFS_REMISE] = F_DOCLIGNE_DEST.[MOTIFS_REMISE]" +
                "      ,[RECEPTION] = F_DOCLIGNE_DEST.[RECEPTION]" +
                "      ,[USERG] = F_DOCLIGNE_DEST.[USERG]" +
                "      ,[DATEMODIF] = F_DOCLIGNE_DEST.[DATEMODIF]" +
                "      ,[CONTROLE] = F_DOCLIGNE_DEST.[CONTROLE]" +
                "      ,[NBJ] = F_DOCLIGNE_DEST.[NBJ]" +
                "      ,[NOM_CLIENT] = F_DOCLIGNE_DEST.[NOM_CLIENT]" +
                "      ,[PMIN] = F_DOCLIGNE_DEST.[PMIN]" +
                "      ,[PMAX] = F_DOCLIGNE_DEST.[PMAX]" +
                "      ,[CONTROLEDATE] = F_DOCLIGNE_DEST.[CONTROLEDATE]" +
                "      ,[PGROS] = F_DOCLIGNE_DEST.[PGROS]" +
                "FROM F_DOCLIGNE_DEST\n" +
                "WHERE\tF_DOCLIGNE.cbMarqSource = F_DOCLIGNE_DEST.cbMarqSource\n" +
                "\tAND\tF_DOCLIGNE.DataBaseSource = F_DOCLIGNE_DEST.DataBaseSource;\n" +
                "            \n" +
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
                "      ,[DE_No],[DL_NoRef],[DL_TypePL],[DL_PUDevise] ,[DL_PUTTC] ,[DL_No],[DO_DateLivr],[CA_Num],[DL_Taxe3] ,[DL_TypeTaux3],[DL_TypeTaxe3],[DL_Frais] \n" +
                "      ,[DL_Valorise],[AR_RefCompose],[DL_NonLivre],[AC_RefClient],[DL_MontantHT] ,[DL_MontantTTC] ,[DL_FactPoids],[DL_Escompte],[DL_PiecePL],[DL_DatePL],[DL_QtePL] ,[DL_NoColis],[DL_NoLink]\n" +
                "      ,[RP_Code],[DL_QteRessource],[DL_DateAvancement],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]" +
                "      ,[MOTIFS_REMISE],[RECEPTION],[USERG],[DATEMODIF],[CONTROLE]"+
                "      ,[NBJ],[NOM_CLIENT],[PMIN],[PMAX],[CONTROLEDATE],[PGROS],dest.[cbMarq],dest.[dataBaseSource]\n" +
                "FROM F_DOCLIGNE_DEST dest\n" +
                "LEFT JOIN (SELECT cbMarqSource,dataBaseSource FROM F_DOCLIGNE) src\n" +
                "\tON\tdest.cbMarqSource = src.cbMarqSource\n" +
                "\tAND\tdest.dataBaseSource = src.dataBaseSource\n" +
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
                "   'F_DOCLIGNE',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }
    public static void sendDataElement(Connection sqlCon, String path,String dataBase)
    {
        dbSource = dataBase;
        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        /*readOnFile(path,file,"F_DOCLIGNE_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"F_DOCLIGNE_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());

         */
        //deleteTempTable(sqlCon);
        deleteDocLigne(sqlCon, path);
    }
    public static void getDataElement(Connection sqlCon, String path,String dataBase)
    {
        dbSource = dataBase;
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        /*initTable(sqlCon);
        getData(sqlCon, list(), "F_DOCLIGNE", path, file);
        listDocLigne(sqlCon, path, "deleteList" + file);

         */
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
    public static void deleteTempTable(Connection sqlCon)
    {
        String query = "IF OBJECT_ID('F_DOCLIGNE_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_DOCLIGNE_DEST;";
        executeQuery(sqlCon, query);
    }
    public static void deleteDocLigne(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_DOCLIGNE \n" +
                " WHERE EXISTS (SELECT 1 FROM F_DOCLIGNE_SUPPR WHERE F_DOCLIGNE.cbMarqSource = F_DOCLIGNE_SUPPR.cbMarq AND F_DOCLIGNE.DataBaseSource = F_DOCLIGNE_SUPPR.DataBaseSource) \n" +
                " \n" +
                " IF OBJECT_ID('F_DOCLIGNE_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_DOCLIGNE_SUPPR \n";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }

    public static void listDocLigne(Connection sqlCon, String path, String file)
    {
        String query = " SELECT lart.DO_Domaine,lart.DO_Type,lart.DO_Piece,[DataBaseSource] = '" + dbSource + "',lart.cbMarq " +
                " FROM config.ListDocLigne lart " +
                " LEFT JOIN dbo.F_DOCLIGNE fart " +
                "    ON lart.cbMarq = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\" + file, query, sqlCon);

        query = " DELETE FROM config.ListDocLigne " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_DOCLIGNE " +
                "                  WHERE dbo.F_DOCLIGNE.cbMarq = config.ListDocLigne.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
