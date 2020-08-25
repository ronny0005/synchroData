import java.io.File;
import java.sql.Connection;

public class EcritureC extends Table {

    public static String file = "EcritureC.csv";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_ECRITUREC";
    public static String configList = "listEcritureC";

    public static String list()
    {
        return   "SELECT [JO_Num],[EC_No],[EC_NoLink],[JM_Date],[EC_Jour]\n" +
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
                "      ,[cbMarq],[cbCreateur],[cbModification],[cbReplication]\n" +
                "      ,[cbFlag],cbMarqSource = [cbMarq]\n" +
                "\t\t,[DataBaseSource] = '" + dbSource + "' \n" +
                "FROM\t[F_ECRITUREC]\n "+
                "WHERE cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='F_ECRITUREC'),'1900-01-01')";
    }

    public static String insert()
    {
        return          "BEGIN TRY" +
                        "\n" +
                        "UPDATE [F_ECRITUREC]\n" +
                        "   SET [EC_NoLink] = F_ECRITUREC_DEST.EC_NoLink\n" +
                        "      ,[EC_Jour] = F_ECRITUREC_DEST.EC_Jour\n" +
                        "      ,[EC_Date] = F_ECRITUREC_DEST.EC_Date\n" +
                        "      ,[EC_Piece] = F_ECRITUREC_DEST.EC_Piece\n" +
                        "      ,[EC_RefPiece] = F_ECRITUREC_DEST.EC_RefPiece\n" +
                        "      ,[EC_TresoPiece] = F_ECRITUREC_DEST.EC_TresoPiece\n" +
                        "      ,[CG_Num] = F_ECRITUREC_DEST.CG_Num\n" +
                        "      ,[CG_NumCont] = F_ECRITUREC_DEST.CG_NumCont\n" +
                        "      ,[CT_Num] = F_ECRITUREC_DEST.CT_Num\n" +
                        "      ,[EC_Intitule] = F_ECRITUREC_DEST.EC_Intitule\n" +
                        "      ,[N_Reglement] = F_ECRITUREC_DEST.N_Reglement\n" +
                        "      ,[EC_Echeance] = F_ECRITUREC_DEST.EC_Echeance\n" +
                        "      ,[EC_Parite] = F_ECRITUREC_DEST.EC_Parite\n" +
                        "      ,[EC_Quantite] = F_ECRITUREC_DEST.EC_Quantite\n" +
                        "      ,[N_Devise] = F_ECRITUREC_DEST.N_Devise\n" +
                        "      ,[EC_Sens] = F_ECRITUREC_DEST.EC_Sens\n" +
                        "      ,[EC_Montant] = F_ECRITUREC_DEST.EC_Montant\n" +
                        "      ,[EC_Lettre] = F_ECRITUREC_DEST.EC_Lettre\n" +
                        "      ,[EC_Lettrage] = F_ECRITUREC_DEST.EC_Lettrage\n" +
                        "      ,[EC_Point] = F_ECRITUREC_DEST.EC_Point\n" +
                        "      ,[EC_Pointage] = F_ECRITUREC_DEST.EC_Pointage\n" +
                        "      ,[EC_Impression] = F_ECRITUREC_DEST.EC_Impression\n" +
                        "      ,[EC_Cloture] = F_ECRITUREC_DEST.EC_Cloture\n" +
                        "      ,[EC_Rappel] = F_ECRITUREC_DEST.EC_Rappel\n" +
                        "      ,[CT_NumCont] = F_ECRITUREC_DEST.CT_NumCont\n" +
                        "      ,[EC_LettreQ] = F_ECRITUREC_DEST.EC_LettreQ\n" +
                        "      ,[EC_LettrageQ] = F_ECRITUREC_DEST.EC_LettrageQ\n" +
                        "      ,[EC_ANType] = F_ECRITUREC_DEST.EC_ANType\n" +
                        "      ,[EC_RType] = F_ECRITUREC_DEST.EC_RType\n" +
                        "      ,[EC_Devise] = F_ECRITUREC_DEST.EC_Devise\n" +
                        "      ,[EC_Remise] = F_ECRITUREC_DEST.EC_Remise\n" +
                        "      ,[EC_ExportExpert] = F_ECRITUREC_DEST.EC_ExportExpert\n" +
                        "      ,[TA_Code] = F_ECRITUREC_DEST.TA_Code\n" +
                        "      ,[EC_Norme] = F_ECRITUREC_DEST.EC_Norme\n" +
                        "      ,[TA_Provenance] = F_ECRITUREC_DEST.TA_Provenance\n" +
                        "      ,[EC_PenalType] = F_ECRITUREC_DEST.EC_PenalType\n" +
                        "      ,[EC_DatePenal] = F_ECRITUREC_DEST.EC_DatePenal\n" +
                        "      ,[EC_DateRelance] = F_ECRITUREC_DEST.EC_DateRelance\n" +
                        "      ,[EC_DateRappro] = F_ECRITUREC_DEST.EC_DateRappro\n" +
                        "      ,[EC_Reference] = F_ECRITUREC_DEST.EC_Reference\n" +
                        "      ,[EC_StatusRegle] = F_ECRITUREC_DEST.EC_StatusRegle\n" +
                        "      ,[EC_MontantRegle] = F_ECRITUREC_DEST.EC_MontantRegle\n" +
                        "      ,[EC_DateRegle] = F_ECRITUREC_DEST.EC_DateRegle\n" +
                        "      ,[EC_RIB] = F_ECRITUREC_DEST.EC_RIB\n" +
                        "      ,[EC_DateOp] = F_ECRITUREC_DEST.EC_DateOp\n" +
                        "      ,[EC_NoCloture] = F_ECRITUREC_DEST.EC_NoCloture\n" +
                        "      ,[cbProt] = F_ECRITUREC_DEST.cbProt\n" +
                        "      ,[cbCreateur] = F_ECRITUREC_DEST.cbCreateur\n" +
                        "      ,[cbModification] = F_ECRITUREC_DEST.cbModification\n" +
                        "      ,[cbReplication] = F_ECRITUREC_DEST.cbReplication\n" +
                        "      ,[cbFlag] = F_ECRITUREC_DEST.cbFlag\n" +
                        "FROM\tF_ECRITUREC_DEST\n" +
                        "WHERE\tF_ECRITUREC.cbMarqSource = F_ECRITUREC_DEST.cbMarq\n" +
                        "AND\t\tF_ECRITUREC.DatabaseSource = F_ECRITUREC_DEST.DatabaseSource\n" +
                        "                   \n" +
                        "INSERT INTO F_ECRITUREC ([JO_Num],[EC_No],[EC_NoLink],[JM_Date],[EC_Jour]\n" +
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
                "SELECT [JO_Num],ISNULL((SELECT Max(EC_No) FROM F_ECRITUREC),0) + ROW_NUMBER() OVER(ORDER BY dest.EC_No),[EC_NoLink],[JM_Date],[EC_Jour]\n" +
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
                "FROM\t[F_ECRITUREC_DEST] dest\n" +
                "LEFT JOIN (SELECT cbMarqSource,DataBaseSource FROM F_ECRITUREC) src\n" +
                "\tON\tdest.cbMarq = src.cbMarqSource \n" +
                "\tAND\tdest.DataBaseSource = src.DataBaseSource\n" +
                "WHERE src.cbMarqSource IS NULL\n" +
                        " END TRY\n" +
                "BEGIN CATCH \n" +
                "INSERT INTO config.DB_Errors\n" +
                "    VALUES\n" +
                "  (SUSER_SNAME(),\n" +
                "   ERROR_NUMBER(),\n" +
                "   ERROR_STATE(),\n" +
                "   ERROR_SEVERITY(),\n" +
                "   ERROR_LINE(),\n" +
                "   ERROR_PROCEDURE(),\n" +
                "   ERROR_MESSAGE(),\n" +
                "   'F_ECRITUREC',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }

    public static void deleteTempTable(Connection  sqlCon)
    {
        String query = "IF OBJECT_ID('F_ECRITUREC_DEST') IS NOT NULL \n" +
                "\tDROP TABLE F_ECRITUREC_DEST;";
        executeQuery(sqlCon, query);
    }
    public static void sendDataElement(Connection  sqlCon, String path,String database)
    {
        dbSource = database;

        readOnFile(path,file,tableName+"_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,tableName+"_SUPPR",sqlCon);
        executeQuery(sqlCon,updateTableDest( "AR_Ref",tableName,tableName+"_DEST"));
        sendData(sqlCon, path, file,selectSourceTable(tableName,"BOUMKO")/*, insert()*/);
        /*
        readOnFile(path,file,"F_ECRITUREC_DEST",sqlCon);
        readOnFile(path,"deleteList"+file,"F_ECRITUREC_SUPPR",sqlCon);
        sendData(sqlCon, path, file, insert());

         */
        deleteTempTable(sqlCon);
        deleteEcritureC(sqlCon, path);
    }
    public static void getDataElement(Connection  sqlCon, String path,String database)
    {
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"AR_Ref,AC_Categorie");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,"BOUMKO")/*list()*/, tableName, path, file);
        listDeleteAllInfo(sqlCon, path, "deleteList" + file,tableName,configList);
        /*initTable(sqlCon);
        getData(sqlCon, list(), "F_ECRITUREC", path, file);
        listDeleteEcritureC(sqlCon, path);

         */
    }
    public static void initTable(Connection  sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_ECRITUREC') \n" +
                "     INSERT INTO config.ListEcritureC\n" +
                "     SELECT EC_No,DataBaseSource ='" + dbSource + "',cbMarq \n" +
                "     FROM F_ECRITUREC";
        executeQuery(sqlCon, query);
    }
    public static void deleteEcritureC(Connection sqlCon, String path)
    {
        String query =
                " DELETE FROM F_ECRITUREC \n" +
                " WHERE EXISTS (SELECT 1 FROM F_ECRITUREC_SUPPR WHERE F_ECRITUREC_SUPPR.EC_No = F_ECRITUREC.EC_No)  \n" +
                "  \n" +
                " IF OBJECT_ID('F_ECRITUREC_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_ECRITUREC_SUPPR ;";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }

    public static void listDeleteEcritureC(Connection sqlCon, String path)
    {
        String query = " SELECT lart.EC_No,lart.cbMarq " +
                " FROM config.ListEcritureC lart " +
                " LEFT JOIN dbo.F_ECRITUREC fart " +
                "    ON lart.cbMarqSource = fart.cbMarq " +
                " WHERE fart.cbMarq IS NULL " +
                ";";

        writeOnFile(path + "\\deleteList" + file, query, sqlCon);

        query = " DELETE FROM config.ListEcritureC " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM F_ECRITUREC " +
                "                  WHERE dbo.F_ECRITUREC.cbMarqSource = config.ListEcritureC.cbMarq);";
        executeQuery(sqlCon, query);
    }
}
