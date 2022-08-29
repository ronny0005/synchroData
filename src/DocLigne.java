import org.json.simple.JSONObject;

import java.sql.Connection;

public class DocLigne extends Table {

    public static String file = "DocLigne_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_DOCLIGNE";
    public static String configList = "listDocLigne";

    public static String insert(String filename)
    {
        return  "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +
                " IF OBJECT_ID('F_DOCLIGNE_DEST') IS NOT NULL\n"+
                "INSERT INTO F_DOCLIGNE (\n" +
                "[DO_Domaine],[DO_Type],[CT_Num],[DO_Piece],[DL_PieceBC],[DL_PieceBL],[DO_Date],[DL_DateBC],[DL_DateBL],[DL_Ligne],[DO_Ref],[DL_TNomencl]\n" +
                "      ,[DL_TRemPied],[DL_TRemExep],[AR_Ref],[DL_Design],[DL_Qte],[DL_QteBC],[DL_QteBL],[DL_PoidsNet] ,[DL_PoidsBrut] \n" +
                "\t  ,[DL_Remise01REM_Valeur] ,[DL_Remise01REM_Type],[DL_Remise02REM_Valeur],[DL_Remise02REM_Type],[DL_Remise03REM_Valeur] ,[DL_Remise03REM_Type]\n" +
                "      ,[DL_PrixUnitaire] ,[DL_PUBC] ,[DL_Taxe1] ,[DL_TypeTaux1],[DL_TypeTaxe1],[DL_Taxe2] ,[DL_TypeTaux2],[DL_TypeTaxe2],[CO_No]/*,[AG_No1],[AG_No2]*/\n" +
                "\t  ,[DL_PrixRU] ,[DL_CMUP] ,[DL_MvtStock],[DT_No],[AF_RefFourniss],[EU_Enumere],[EU_Qte] ,[DL_TTC]\n" +
                "      ,[DE_No],[DL_NoRef],[DL_TypePL],[DL_PUDevise] ,[DL_PUTTC] ,[DL_No],[DO_DateLivr],[CA_Num],[DL_Taxe3] ,[DL_TypeTaux3],[DL_TypeTaxe3],[DL_Frais] \n" +
                "      ,[DL_Valorise],[AR_RefCompose],[DL_NonLivre],[AC_RefClient],[DL_MontantHT] ,[DL_MontantTTC] ,[DL_FactPoids],[DL_Escompte],[DL_PiecePL],[DL_DatePL],[DL_QtePL] ,[DL_NoColis],[DL_NoLink]\n" +
                "      ,[RP_Code],[DL_QteRessource],[DL_DateAvancement],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]" +
                "      ,[DATEMODIF]" +
                //",[CONTROLE],[NBJ],[NOM_CLIENT],[PMIN],[PMAX],[CONTROLEDATE],[PGROS]" +
                ",[DL_QTE_SUPER_PRIX]" +
                "      ,[DL_SUPER_PRIX],[USERGESCOM],[NOMCLIENT],[ORDONATEUR_REMISE],[GROUPEUSER],[Qte_LivreeBL],[Qte_RestantBL],[DL_COMM],[cbMarqSource],[DataBaseSource]" +
                "      )\n" +
                "            \n" +
                "SELECT\tdest.[DO_Domaine],dest.[DO_Type],dest.[CT_Num],dest.[DO_Piece],dest.[DL_PieceBC],dest.[DL_PieceBL],dest.[DO_Date],dest.[DL_DateBC]" +
                "       ,dest.[DL_DateBL],dest.[DL_Ligne],LEFT(CAST(dest.[DO_Ref] AS VARCHAR(150)),17),dest.[DL_TNomencl]\n" +
                "       ,dest.[DL_TRemPied],dest.[DL_TRemExep],dest.[AR_Ref],LEFT(dest.[DL_Design],69),dest.[DL_Qte],dest.[DL_QteBC],dest.[DL_QteBL]" +
                "       ,dest.[DL_PoidsNet] ,dest.[DL_PoidsBrut] \n" +
                "\t     ,dest.[DL_Remise01REM_Valeur] ,dest.[DL_Remise01REM_Type],dest.[DL_Remise02REM_Valeur],dest.[DL_Remise02REM_Type],dest.[DL_Remise03REM_Valeur] " +
                "       ,dest.[DL_Remise03REM_Type]\n" +
                "       ,dest.[DL_PrixUnitaire] ,dest.[DL_PUBC] ,dest.[DL_Taxe1] ,dest.[DL_TypeTaux1],dest.[DL_TypeTaxe1],dest.[DL_Taxe2] ,dest.[DL_TypeTaux2]" +
                "       ,dest.[DL_TypeTaxe2],dest.[CO_No]/*,[AG_No1],[AG_No2]*/\n" +
                "\t     ,dest.[DL_PrixRU] ,dest.[DL_CMUP] ,dest.[DL_MvtStock],dest.[DT_No],dest.[AF_RefFourniss],dest.[EU_Enumere],dest.[EU_Qte] ,dest.[DL_TTC]\n" +
                "       ,ISNULL(dsrc.[DE_No],dest.DE_No),dest.[DL_NoRef],dest.[DL_TypePL],dest.[DL_PUDevise] ,dest.[DL_PUTTC] " +
                "       ,ISNULL((SELECT MAX(DL_No) FROM F_DOCLIGNE),0)+ ROW_NUMBER() OVER (ORDER BY dest.[cbMarqSource]),dest.[DO_DateLivr],dest.[CA_Num]" +
                "       ,dest.[DL_Taxe3] ,dest.[DL_TypeTaux3],dest.[DL_TypeTaxe3],dest.[DL_Frais] \n" +
                "      ,[DL_Valorise],[AR_RefCompose],[DL_NonLivre],[AC_RefClient],[DL_MontantHT] ,[DL_MontantTTC] ,[DL_FactPoids],[DL_Escompte],[DL_PiecePL],[DL_DatePL],[DL_QtePL] ,[DL_NoColis],[DL_NoLink]\n" +
                "      ,[RP_Code],[DL_QteRessource],[DL_DateAvancement],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]" +
                "      ,[DATEMODIF]" +
               // "       ,[CONTROLE],[NBJ],[NOM_CLIENT],[PMIN],[PMAX],[CONTROLEDATE],[PGROS]" +
                "       ,[DL_QTE_SUPER_PRIX]"+
                "      ,[DL_SUPER_PRIX],[USERGESCOM],[NOMCLIENT],[ORDONATEUR_REMISE],[GROUPEUSER],[Qte_LivreeBL],[Qte_RestantBL],[DL_COMM],dest.[cbMarqSource],dest.[DataBaseSource]\n" +
                "FROM F_DOCLIGNE_DEST dest\n" +
                "LEFT JOIN (SELECT cbMarqSource,dataBaseSource,DO_Piece,DO_Type,DO_Domaine FROM F_DOCLIGNE) src\n" +
                "\tON\tdest.cbMarqSource = src.cbMarqSource\n" +
                "\tAND\tdest.dataBaseSource = src.dataBaseSource\n" +
                "LEFT JOIN (SELECT DE_NoSource,dataBaseSource,DE_No FROM F_DEPOT) dsrc\n" +
                "\tON\tdsrc.DE_NoSource = dest.DE_No\n" +
                "\tAND\tdsrc.dataBaseSource = dest.dataBaseSource\n" +
                "WHERE src.cbMarqSource IS NULL\n" +
                "AND EXISTS (   SELECT 1\n" +
                "               FROM F_DOCENTETE ent\n" +
                "               WHERE ent.DO_Piece = dest.DO_Piece\n" +
                "               AND ent.DO_Type = dest.DO_Type\n" +
                "               AND ent.DO_Domaine = dest.DO_Domaine);\n" +
                "\n" +
                "INSERT INTO config.DB_Errors(\n" +
                "          UserName,\n" +
                "          ErrorNumber,\n" +
                "          ErrorState,\n" +
                "          ErrorSeverity,\n" +
                "          ErrorLine,\n" +
                "          ErrorProcedure,\n" +
                "          ErrorMessage,\n" +
                "          TableLoad,\n" +
                "          Query,\n" +
                "          ErrorDateTime)\n" +

                "SELECT\t   SUSER_SNAME()," +
                "           NULL," +
                "           NULL," +
                "           NULL," +
                "           NULL," +
                "           NULL," +
                "           NULL,'Domaine : '+[DO_Domaine]+' Type : ' + [DO_Type] + ' Piece : ' + [DO_Piece]" +
                "           +' cbMarq : ' + dest.[cbMarqSource] + ' database : ' + dest.[DataBaseSource]" +
                "           + 'fileName : "+filename+" '" +
                "           ,'Insert F_DOCLIGNE'\n" +
                "           ,GETDATE()\n" +
                "FROM F_DOCLIGNE_DEST dest\n" +
                "LEFT JOIN (SELECT cbMarqSource,dataBaseSource FROM F_DOCLIGNE) src\n" +
                "\tON\tdest.cbMarqSource = src.cbMarqSource\n" +
                "\tAND\tdest.dataBaseSource = src.dataBaseSource\n" +
                "LEFT JOIN (SELECT DE_NoSource,dataBaseSource,DE_No FROM F_DEPOT) dsrc\n" +
                "\tON\tdsrc.DE_NoSource = dest.DE_No\n" +
                "\tAND\tdsrc.dataBaseSource = dest.dataBaseSource\n" +
                "WHERE src.cbMarqSource IS NULL\n" +
                "AND NOT EXISTS (   SELECT 1\n" +
                "               FROM F_DOCENTETE ent\n" +
                "               WHERE ent.DO_Piece = dest.DO_Piece\n" +
                "               AND ent.DO_Type = dest.DO_Type\n" +
                "               AND ent.DO_Domaine = dest.DO_Domaine);\n" +
                "            \n" +
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
                "   'F_DOCLIGNE',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }
    public static void sendDataElement(Connection sqlCon, String path,String database)
    {
        dbSource = database;
        loadFile(path,sqlCon);
        loadDeleteFile(path,sqlCon);
        DocEntete.loadDeleteFile(path,sqlCon);
    }

    public static void loadFile(String path,Connection sqlCon){
        String [] children = getFile(path,file);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children){
                disableTrigger(sqlCon,tableName);
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                executeQuery(sqlCon, updateDocLigne(filename));
                sendData(sqlCon, path, filename, insert(filename));
                enableTrigger(sqlCon,tableName);
                //deleteTempTable(sqlCon, tableName+"_DEST");
            }
        }
    }

    public static String updateDocLigne(String filename){
        return "BEGIN TRY\n" +
                "\tSET DATEFORMAT ymd;\n" +
                "\n" +
                "\tIF OBJECT_ID('F_DOCLIGNE_DEST') IS NOT NULL\n" +
                "\tBEGIN\n" +
                "\t\tUPDATE F_DOCLIGNE\n" +
                "\t\tSET DO_Domaine = docL.DO_Domaine\n" +
                "\t\t\t,DO_Type = docL.DO_Type\n" +
                "\t\t\t,CT_Num = docL.CT_Num\n" +
                "\t\t\t,DO_Piece = docL.DO_Piece\n" +
                "\t\t\t,DL_PieceBC = docL.DL_PieceBC\n" +
                "\t\t\t,DL_PieceBL = docL.DL_PieceBL\n" +
                "\t\t\t,DO_Date = docL.DO_Date\n" +
                "\t\t\t,DL_DateBC = docL.DL_DateBC\n" +
                "\t\t\t,DL_DateBL = docL.DL_DateBL\n" +
                "\t\t\t,DL_Ligne = docL.DL_Ligne\n" +
                "\t\t\t,DO_Ref = docL.DO_Ref\n" +
                "\t\t\t,DL_TNomencl = docL.DL_TNomencl\n" +
                "\t\t\t,DL_TRemPied = docL.DL_TRemPied\n" +
                "\t\t\t,DL_TRemExep = docL.DL_TRemExep\n" +
                "\t\t\t,DL_Design = docL.DL_Design\n" +
                "\t\t\t,DL_Qte = docL.DL_Qte\n" +
                "\t\t\t,DL_QteBC = docL.DL_QteBC\n" +
                "\t\t\t,DL_QteBL = docL.DL_QteBL\n" +
                "\t\t\t,DL_PoidsNet = docL.DL_PoidsNet\n" +
                "\t\t\t,DL_PoidsBrut = docL.DL_PoidsBrut\n" +
                "\t\t\t,DL_Remise01REM_Valeur = docL.DL_Remise01REM_Valeur\n" +
                "\t\t\t,DL_Remise01REM_Type = docL.DL_Remise01REM_Type\n" +
                "\t\t\t,DL_Remise02REM_Valeur = docL.DL_Remise02REM_Valeur\n" +
                "\t\t\t,DL_Remise02REM_Type = docL.DL_Remise02REM_Type\n" +
                "\t\t\t,DL_Remise03REM_Valeur = docL.DL_Remise03REM_Valeur\n" +
                "\t\t\t,DL_Remise03REM_Type = docL.DL_Remise03REM_Type\n" +
                "\t\t\t,DL_PrixUnitaire = docL.DL_PrixUnitaire\n" +
                "\t\t\t,DL_PUBC = docL.DL_PUBC\n" +
                "\t\t\t,DL_Taxe1 = docL.DL_Taxe1\n" +
                "\t\t\t,DL_TypeTaux1 = docL.DL_TypeTaux1\n" +
                "\t\t\t,DL_TypeTaxe1 = docL.DL_TypeTaxe1\n" +
                "\t\t\t,DL_Taxe2 = docL.DL_Taxe2\n" +
                "\t\t\t,DL_TypeTaux2 = docL.DL_TypeTaux2\n" +
                "\t\t\t,DL_TypeTaxe2 = docL.DL_TypeTaxe2\n" +
                "\t\t\t,CO_No = docL.CO_No\n" +
                "\t\t\t,DL_PrixRU = docL.DL_PrixRU\n" +
                "\t\t\t,DL_CMUP = docL.DL_CMUP\n" +
                "\t\t\t,DL_MvtStock = docL.DL_MvtStock\n" +
                "\t\t\t,DT_No = docL.DT_No\n" +
                "\t\t\t,AF_RefFourniss = docL.AF_RefFourniss\n" +
                "\t\t\t,EU_Enumere = docL.EU_Enumere\n" +
                "\t\t\t,EU_Qte = docL.EU_Qte\n" +
                "\t\t\t,DL_TTC = docL.DL_TTC\n" +
                "\t\t\t,DE_No = docL.DE_No\n" +
                "\t\t\t,DL_NoRef = docL.DL_NoRef\n" +
                "\t\t\t,DL_TypePL = docL.DL_TypePL\n" +
                "\t\t\t,DL_PUDevise = docL.DL_PUDevise\n" +
                "\t\t\t,DL_PUTTC = docL.DL_PUTTC\n" +
                "\t\t\t,DO_DateLivr = docL.DO_DateLivr\n" +
                "\t\t\t,CA_Num = docL.CA_Num\n" +
                "\t\t\t,DL_Taxe3 = docL.DL_Taxe3\n" +
                "\t\t\t,DL_TypeTaux3 = docL.DL_TypeTaux3\n" +
                "\t\t\t,DL_TypeTaxe3 = docL.DL_TypeTaxe3\n" +
                "\t\t\t,DL_Frais = docL.DL_Frais\n" +
                "\t\t\t,DL_Valorise = docL.DL_Valorise\n" +
                "\t\t\t,AR_RefCompose = docL.AR_RefCompose\n" +
                "\t\t\t,DL_NonLivre = docL.DL_NonLivre\n" +
                "\t\t\t,AC_RefClient = docL.AC_RefClient\n" +
                "\t\t\t,DL_MontantHT = docL.DL_MontantHT\n" +
                "\t\t\t,DL_MontantTTC = docL.DL_MontantTTC\n" +
                "\t\t\t,DL_FactPoids = docL.DL_FactPoids\n" +
                "\t\t\t,DL_Escompte = docL.DL_Escompte\n" +
                "\t\t\t,DL_PiecePL = docL.DL_PiecePL\n" +
                "\t\t\t,DL_DatePL = docL.DL_DatePL\n" +
                "\t\t\t,DL_QtePL = docL.DL_QtePL\n" +
                "\t\t\t,DL_NoColis = docL.DL_NoColis\n" +
                "\t\t\t,DL_NoLink = docL.DL_NoLink\n" +
                "\t\t\t,RP_Code = docL.RP_Code\n" +
                "\t\t\t,DL_QteRessource = docL.DL_QteRessource\n" +
                "\t\t\t,DL_DateAvancement = docL.DL_DateAvancement\n" +
                "\t\t\t,DATEMODIF = docL.DATEMODIF\n" +
                /*"\t\t\t,CONTROLE = docL.CONTROLE\n" +
                "\t\t\t,NBJ = docL.NBJ\n" +
                "\t\t\t,NOM_CLIENT = docL.NOM_CLIENT\n" +
                "\t\t\t,PMIN = docL.PMIN\n" +
                "\t\t\t,PMAX = docL.PMAX\n" +
                "\t\t\t,CONTROLEDATE = docL.CONTROLEDATE\n" +
                "\t\t\t,PGROS = docL.PGROS\n" +*/
                "\t\t\t,DL_QTE_SUPER_PRIX = docL.DL_QTE_SUPER_PRIX\n" +
                "\t\t\t,DL_SUPER_PRIX = docL.DL_SUPER_PRIX\n" +
                "\t\t\t,NOMCLIENT = docL.NOMCLIENT\n" +
                "\t\t\t,ORDONATEUR_REMISE = docL.ORDONATEUR_REMISE\n" +
                "\t\t\t,GROUPEUSER = docL.GROUPEUSER\n" +
                "\t\t\t,Qte_LivreeBL = docL.Qte_LivreeBL\n" +
                "\t\t\t,Qte_RestantBL = docL.Qte_RestantBL\n" +
                "\t\t\t,DL_COMM = docL.DL_COMM\n" +
                "\t\t\t,DataBaseSource = docL.DataBaseSource\n" +
                "\t\t\t,USERGESCOM = docL.USERGESCOM\n" +
                "\t\t\t,MACHINEPC = docL.MACHINEPC\n" +
                "\t\tFROM F_DOCLIGNE_DEST docL\n" +
                "\t\tWHERE F_DOCLIGNE.cbMarqSource = docL.cbMarqSource\n" +
                "\t\t\tAND F_DOCLIGNE.DataBaseSource = docL.DataBaseSource\n" +
                "\t\t\tAND EXISTS (\n" +
                "\t\t\t\tSELECT 1\n" +
                "\t\t\t\tFROM F_DOCENTETE docE\n" +
                "\t\t\t\tWHERE docE.DO_Type = docL.DO_Type\n" +
                "\t\t\t\t\tAND docE.DO_Piece = docL.DO_Piece\n" +
                "\t\t\t\t\tAND docE.DO_Domaine = docL.DO_Domaine\n" +
                "\t\t\t\t)\n" +
                "\n" +
                "\t\tINSERT INTO config.DB_Errors (\n" +
                "\t\t\tUserName\n" +
                "\t\t\t,ErrorNumber\n" +
                "\t\t\t,ErrorState\n" +
                "\t\t\t,ErrorSeverity\n" +
                "\t\t\t,ErrorLine\n" +
                "\t\t\t,ErrorProcedure\n" +
                "\t\t\t,ErrorMessage\n" +
                "\t\t\t,TableLoad\n" +
                "\t\t\t,Query\n" +
                "\t\t\t,ErrorDateTime\n" +
                "\t\t\t)\n" +
                "\t\tSELECT SUSER_SNAME()\n" +
                "\t\t\t,NULL\n" +
                "\t\t\t,NULL\n" +
                "\t\t\t,NULL\n" +
                "\t\t\t,NULL\n" +
                "\t\t\t,NULL\n" +
                "\t\t\t,NULL\n" +
                "\t\t\t,'Domaine : ' + dest.[DO_Domaine] + ' Type : ' + dest.[DO_Type] + ' Piece : ' + dest.[DO_Piece] + ' cbMarq : ' + dest.[cbMarqSource] + ' database : ' + dest.[DataBaseSource] + 'fileName : "+filename+" '\n" +
                "\t\t\t,'Update F_DOCLIGNE'\n" +
                "\t\t\t,GETDATE()\n" +
                "\t\tFROM F_DOCLIGNE_DEST dest\n" +
                "\t\tINNER JOIN F_DOCLIGNE docL ON docL.cbMarqSource = dest.cbMarqSource\n" +
                "\t\t\tAND docL.DataBaseSource = dest.DataBaseSource\n" +
                "\t\t\tAND NOT EXISTS (\n" +
                "\t\t\t\tSELECT 1\n" +
                "\t\t\t\tFROM F_DOCENTETE docE\n" +
                "\t\t\t\tWHERE docE.DO_Type = docL.DO_Type\n" +
                "\t\t\t\t\tAND docE.DO_Piece = docL.DO_Piece\n" +
                "\t\t\t\t\tAND docE.DO_Domaine = docL.DO_Domaine\n" +
                "\t\t\t\t)\n" +
                "\tEND\n" +
                "END TRY\n" +
                "\n" +
                "BEGIN CATCH\n" +
                "\tINSERT INTO config.DB_Errors\n" +
                "\tVALUES (\n" +
                "\t\tSUSER_SNAME()\n" +
                "\t\t,ERROR_NUMBER()\n" +
                "\t\t,ERROR_STATE()\n" +
                "\t\t,ERROR_SEVERITY()\n" +
                "\t\t,ERROR_LINE()\n" +
                "\t\t,ERROR_PROCEDURE()\n" +
                "\t\t,ERROR_MESSAGE()\n" +
                "\t\t,'Insert ' + ' "+filename+"'\n" +
                "\t\t,'F_DOCLIGNE'\n" +
                "\t\t,GETDATE()\n" +
                "\t\t);\n" +
                "END CATCH\n";
    }
    public static void loadDeleteFile(String path,Connection sqlCon) {
        String [] children = getFile(path,"deleteList"+file);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children){
                String deleteDocLigne = deleteDocLigne();
                loadDeleteInfo(path,tableName,filename,sqlCon,deleteDocLigne);
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

    public static String deleteDocLigne()
    {
        return
                " DELETE FROM F_DOCLIGNE \n" +
                " WHERE EXISTS (SELECT 1 " +
                "               FROM F_DOCLIGNE_SUPPR " +
                "               WHERE F_DOCLIGNE.cbMarqSource = F_DOCLIGNE_SUPPR.cbMarq " +
                "               AND F_DOCLIGNE.DataBaseSource = F_DOCLIGNE_SUPPR.DataBaseSource) \n"
                ;

    }
}
