import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class EcritureC extends Table {

    public static String file = "EcritureC_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_ECRITUREC";
    public static String configList = "listEcritureC";

    public static String insert(String filename)
    {
        return          "BEGIN TRY " +

                " IF OBJECT_ID('F_ECRITUREC_DEST') IS NOT NULL\n"+
                " INSERT INTO F_ECRITUREC ([JO_Num],[EC_No],[EC_NoLink],[JM_Date],[EC_Jour]\n" +
                "      ,[EC_Date],[EC_Piece],[EC_RefPiece],[EC_TresoPiece]\n" +
                "      ,[CG_Num],[CG_NumCont],[CT_Num],[EC_Intitule]\n" +
                "      ,[N_Reglement],[EC_Echeance],[EC_Parite],[EC_Quantite]\n" +
                "      ,[N_Devise],[EC_Sens],[EC_Montant],[EC_Lettre]\n" +
                "      ,[EC_Lettrage],[EC_Point],[EC_Pointage],[EC_Impression]\n" +
                "      ,[EC_Cloture],[EC_CType],[EC_Rappel],[CT_NumCont]\n" +
                "      ,[EC_LettreQ],[EC_LettrageQ],[EC_ANType],[EC_RType]\n" +
                "      ,[EC_Devise],[EC_Remise],[EC_ExportExpert],[TA_Code]\n" +
                "      ,[EC_Norme],[TA_Provenance],[EC_PenalType],[EC_DatePenal],[EC_DateRelance],[EC_DateRappro]\n" +
                "      ,[EC_Reference],[EC_StatusRegle],[EC_MontantRegle],[EC_DateRegle]\n" +
                "      ,[EC_RIB],[EC_DateOp],[EC_NoCloture],[cbProt]\n" +
                "      ,[cbMarqSource],[cbCreateur],[cbModification],[cbReplication]\n" +
                "      ,[cbFlag],DatabaseSource,EC_NoSource)\n" +
                "SELECT [JO_Num],ISNULL((SELECT Max(EC_No) FROM F_ECRITUREC),0) + ROW_NUMBER() OVER(ORDER BY dest.EC_No)" +
                "       ,[EC_NoLink],[JM_Date],[EC_Jour]\n" +
                "      ,[EC_Date],[EC_Piece],[EC_RefPiece],[EC_TresoPiece]\n" +
                "      ,[CG_Num],[CG_NumCont],[CT_Num],[EC_Intitule]\n" +
                "      ,[N_Reglement],[EC_Echeance],[EC_Parite],[EC_Quantite]\n" +
                "      ,[N_Devise],[EC_Sens],[EC_Montant],[EC_Lettre]\n" +
                "      ,[EC_Lettrage],[EC_Point],[EC_Pointage],[EC_Impression]\n" +
                "      ,[EC_Cloture],[EC_CType],[EC_Rappel],[CT_NumCont]\n" +
                "      ,[EC_LettreQ],[EC_LettrageQ],[EC_ANType],[EC_RType]\n" +
                "      ,[EC_Devise],[EC_Remise],[EC_ExportExpert],[TA_Code]\n" +
                "      ,[EC_Norme],[TA_Provenance],[EC_PenalType],[EC_DatePenal],[EC_DateRelance],[EC_DateRappro]\n" +
                "      ,[EC_Reference],[EC_StatusRegle],[EC_MontantRegle],[EC_DateRegle]\n" +
                "      ,[EC_RIB],[EC_DateOp],[EC_NoCloture],[cbProt]\n" +
                "      ,dest.[cbMarqSource],[cbCreateur],[cbModification],[cbReplication]\n" +
                "      ,[cbFlag],dest.DatabaseSource,[EC_No]\n" +
                " FROM\t[F_ECRITUREC_DEST] dest\n" +
                " LEFT JOIN (SELECT cbMarqSource,DataBaseSource FROM F_ECRITUREC) src\n" +
                " \tON\tISNULL(dest.cbMarqSource,0) = ISNULL(src.cbMarqSource,0) \n" +
                " \tAND\tISNULL(dest.DataBaseSource,'') = ISNULL(src.DataBaseSource,'')\n" +
                " WHERE src.cbMarqSource IS NULL" +
                " AND dest.EC_NoLink = 0;\n" +

                " INSERT INTO F_ECRITUREC ([JO_Num],[EC_No],[EC_NoLink],[JM_Date],[EC_Jour]\n" +
                "      ,[EC_Date],[EC_Piece],[EC_RefPiece],[EC_TresoPiece]\n" +
                "      ,[CG_Num],[CG_NumCont],[CT_Num],[EC_Intitule]\n" +
                "      ,[N_Reglement],[EC_Echeance],[EC_Parite],[EC_Quantite]\n" +
                "      ,[N_Devise],[EC_Sens],[EC_Montant],[EC_Lettre]\n" +
                "      ,[EC_Lettrage],[EC_Point],[EC_Pointage],[EC_Impression]\n" +
                "      ,[EC_Cloture],[EC_CType],[EC_Rappel],[CT_NumCont]\n" +
                "      ,[EC_LettreQ],[EC_LettrageQ],[EC_ANType],[EC_RType]\n" +
                "      ,[EC_Devise],[EC_Remise],[EC_ExportExpert],[TA_Code]\n" +
                "      ,[EC_Norme],[TA_Provenance],[EC_PenalType],[EC_DatePenal],[EC_DateRelance],[EC_DateRappro]\n" +
                "      ,[EC_Reference],[EC_StatusRegle],[EC_MontantRegle],[EC_DateRegle]\n" +
                "      ,[EC_RIB],[EC_DateOp],[EC_NoCloture],[cbProt]\n" +
                "      ,[cbMarqSource],[cbCreateur],[cbModification],[cbReplication]\n" +
                "      ,[cbFlag],DatabaseSource,EC_NoSource)\n" +
                "SELECT [JO_Num],ISNULL((SELECT Max(EC_No) FROM F_ECRITUREC),0) + ROW_NUMBER() OVER(ORDER BY dest.EC_No)" +
                "       ,[EC_NoLink],[JM_Date],[EC_Jour]\n" +
                "      ,[EC_Date],[EC_Piece],[EC_RefPiece],[EC_TresoPiece]\n" +
                "      ,[CG_Num],[CG_NumCont],[CT_Num],[EC_Intitule]\n" +
                "      ,[N_Reglement],[EC_Echeance],[EC_Parite],[EC_Quantite]\n" +
                "      ,[N_Devise],[EC_Sens],[EC_Montant],[EC_Lettre]\n" +
                "      ,[EC_Lettrage],[EC_Point],[EC_Pointage],[EC_Impression]\n" +
                "      ,[EC_Cloture],[EC_CType],[EC_Rappel],[CT_NumCont]\n" +
                "      ,[EC_LettreQ],[EC_LettrageQ],[EC_ANType],[EC_RType]\n" +
                "      ,[EC_Devise],[EC_Remise],[EC_ExportExpert],[TA_Code]\n" +
                "      ,[EC_Norme],[TA_Provenance],[EC_PenalType],[EC_DatePenal],[EC_DateRelance],[EC_DateRappro]\n" +
                "      ,[EC_Reference],[EC_StatusRegle],[EC_MontantRegle],[EC_DateRegle]\n" +
                "      ,[EC_RIB],[EC_DateOp],[EC_NoCloture],[cbProt]\n" +
                "      ,dest.[cbMarqSource],[cbCreateur],[cbModification],[cbReplication]\n" +
                "      ,[cbFlag],dest.DatabaseSource,[EC_No]\n" +
                " FROM\t[F_ECRITUREC_DEST] dest\n" +
                " LEFT JOIN (SELECT cbMarqSource,DataBaseSource FROM F_ECRITUREC) src\n" +
                " \tON\tISNULL(dest.cbMarqSource,0) = ISNULL(src.cbMarqSource,0) \n" +
                " \tAND\tISNULL(dest.DataBaseSource,'') = ISNULL(src.DataBaseSource,'')\n" +
                " WHERE src.cbMarqSource IS NULL" +
                " AND dest.EC_NoLink <> 0;\n" +
                        " END TRY\n" +
                " BEGIN CATCH \n" +
                " INSERT INTO config.DB_Errors\n" +
                "    VALUES\n" +
                "  (SUSER_SNAME(),\n" +
                "   ERROR_NUMBER(),\n" +
                "   ERROR_STATE(),\n" +
                "   ERROR_SEVERITY(),\n" +
                "   ERROR_LINE(),\n" +
                "   ERROR_PROCEDURE(),\n" +
                "   ERROR_MESSAGE(),\n" +
                "   'Insert '+ ' "+filename+"',\n" +
                "   'F_ECRITUREC',\n" +
                "   GETDATE());\n" +
                " END CATCH";
    }

    public static void sendDataElement(Connection  sqlCon, String path,String database,int unibase)
    {
        File dir = new File(path);
        FilenameFilter filter = (dir1, name) -> name.startsWith(file);
        String[] children = dir.list(filter);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                dbSource = database;

                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                readOnFile(path, "deleteList" + filename, tableName + "_SUPPR", sqlCon);
                executeQuery(sqlCon, updateTableDest("", "'EC_No','JM_Date','JO_Num','EC_CType'", tableName, tableName + "_DEST", filename,unibase));
                sendData(sqlCon, path, filename, insert(filename));

                executeQuery(sqlCon,insertTable (tableName,tableName+"_DEST","cbMarqSource,dataBaseSource",filename,1,1,"RG_No","RG_No",""));

                deleteTempTable(sqlCon, tableName + "_DEST");

                deleteEcritureC(sqlCon, path, filename);
            }
        }
    }
    public static void getDataElement(Connection  sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"EC_No,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,true), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }

    public static void deleteEcritureC(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_ECRITUREC \n" +
                " WHERE EXISTS (SELECT 1 FROM F_ECRITUREC_SUPPR WHERE ISNULL(F_ECRITUREC_SUPPR.EC_No,0) = ISNULL(F_ECRITUREC.EC_NoSource,0) AND ISNULL(F_ECRITUREC_SUPPR.DatabaseSource,'') = ISNULL(F_ECRITUREC.DatabaseSource,''))  \n" +
                "  \n" +
                " IF OBJECT_ID('F_ECRITUREC_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_ECRITUREC_SUPPR ;";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }

}
