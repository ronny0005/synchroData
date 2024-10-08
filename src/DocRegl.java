import org.json.simple.JSONObject;

import java.sql.Connection;

public class DocRegl extends Table {

    public static String file = "docRegl_";
    public static String dbSource = "BIJOU";
    public static String tableName = "F_DOCREGL";
    public static String configList = "listDocRegl";

    public static String insert(String filename)
    {
        return  "BEGIN TRY " +
                " SET DATEFORMAT ymd;\n" +

                " IF OBJECT_ID('F_DOCREGL_DEST') IS NOT NULL\n"+
                " UPDATE\tF_DOCREGL_DEST\n" +
                "SET\t[DR_Pourcent] = REPLACE([DR_Pourcent],',','.')\n" +
                "\t,[DR_Montant] = REPLACE([DR_Montant],',','.')\n" +
                "\t, [DR_MontantDev] = REPLACE([DR_MontantDev],',','.')\n" +
                "\n" +

                " IF OBJECT_ID('F_DOCREGL_DEST') IS NOT NULL\n"+
                "UPDATE src\n" +
                "   SET [DR_TypeRegl] = dest.DR_TypeRegl\n" +
                "      ,[DR_Date] = dest.DR_Date\n" +
                "      ,[DR_Libelle] = dest.DR_Libelle\n" +
                "      ,[DR_Pourcent] = dest.DR_Pourcent\n" +
                "      ,[DR_Montant] = dest.DR_Montant\n" +
                "      ,[DR_MontantDev] = dest.DR_MontantDev\n" +
                "      ,[DR_Equil] = dest.DR_Equil\n" +
                "      ,[EC_No] = dest.EC_No\n" +
                "      ,[DR_Regle] = dest.DR_Regle\n" +
                "      ,[N_Reglement] = dest.N_Reglement\n" +
                "      ,[cbProt] = dest.cbProt\n" +
                "      ,[cbCreateur] = dest.cbCreateur\n" +
                "      ,[cbModification] = dest.cbModification\n" +
                "      ,[cbReplication] = dest.cbReplication\n" +
                "      ,[cbFlag] = dest.cbFlag\n" +
                "      ,[DR_NoSource] = dest.DR_No\n" +
                "FROM [dbo].[F_DOCREGL] src\n" +
                "INNER JOIN F_DOCREGL_DEST dest ON src.cbMarqSource = dest.cbMarqSource \n" +
                "\tAND\tISNULL(src.DataBaseSource,'') = ISNULL(dest.DataBaseSource,'')\n" +
                "            \n" +

                " IF OBJECT_ID('F_DOCREGL_DEST') IS NOT NULL\n"+
                "INSERT INTO F_DOCREGL (\n" +
                "[DR_No],[DO_Domaine],[DO_Type],[DO_Piece],[DR_TypeRegl],[DR_Date]\n" +
                "\t\t,[DR_Libelle],[DR_Pourcent],[DR_Montant],[DR_MontantDev],[DR_Equil],[EC_No],[DR_Regle]\n" +
                "\t\t,[N_Reglement],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag],cbMarqSource,DataBaseSource,DR_NoSource)\n" +
                "            \n" +
                "SELECT \tISNULL((SELECT Max(DR_No) FROM F_DOCREGL),0) + ROW_NUMBER() OVER(ORDER BY dest.DR_No)\n" +
                "\t\t,dest.[DO_Domaine],dest.[DO_Type],dest.[DO_Piece],[DR_TypeRegl],[DR_Date]\n" +
                "\t\t,[DR_Libelle],[DR_Pourcent],[DR_Montant],[DR_MontantDev],[DR_Equil],[EC_No],[DR_Regle]\n" +
                "\t\t,[N_Reglement],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag],dest.cbMarqSource,dest.[dataBaseSource],dest.DR_No\n" +
                "FROM F_DOCREGL_DEST dest\n" +
                "LEFT JOIN (SELECT cbMarqSource,DataBaseSource FROM F_DOCREGL) src\n" +
                "\tON\tISNULL(dest.cbMarqSource,0) = ISNULL(src.cbMarqSource,0)\n" +
                "\tAND\tISNULL(dest.DataBaseSource,'') = ISNULL(src.DataBaseSource,'')\n" +
                "LEFT JOIN (SELECT DO_Piece,DO_Type,DO_Domaine FROM F_DOCENTETE) docE\n" +
                "ON docE.DO_Domaine = dest.DO_Domaine\n"+
                "AND docE.DO_Type = dest.DO_Type\n"+
                "AND docE.DO_Piece = dest.DO_Piece\n"+
                "WHERE src.cbMarqSource IS NULL\n" +
                "AND docE.DO_Piece IS NOT NULL\n" +
                "            \n" +
                "" +
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
                "           NULL,'Domaine : '+CAST(dest.[DO_Domaine] AS VARCHAR(150))+' Type : ' + CAST(dest.[DO_Type]  AS VARCHAR(150)) + ' Piece : ' + CAST(dest.[DO_Piece] AS VARCHAR(150))" +
                "           +' cbMarq : ' + CAST(dest.[cbMarqSource] AS VARCHAR(150)) + ' database : ' + CAST(dest.[DataBaseSource] AS VARCHAR(150))" +
                "           + 'fileName : "+filename+" '" +
                "           ,'F_DOCREGL'\n" +
                "           ,GETDATE()\n" +
                "FROM F_DOCREGL_DEST dest\n" +
                "LEFT JOIN (SELECT cbMarqSource,DataBaseSource FROM F_DOCREGL) src\n" +
                "\tON\tISNULL(dest.cbMarqSource,0) = ISNULL(src.cbMarqSource,0)\n" +
                "\tAND\tISNULL(dest.DataBaseSource,'') = ISNULL(src.DataBaseSource,'')\n" +
                "LEFT JOIN (SELECT DO_Piece,DO_Type,DO_Domaine FROM F_DOCENTETE) docE\n" +
                "ON docE.DO_Domaine = dest.DO_Domaine\n"+
                "AND docE.DO_Type = dest.DO_Type\n"+
                "AND docE.DO_Piece = dest.DO_Piece\n"+
                "WHERE src.cbMarqSource IS NULL\n" +
                "AND docE.DO_Piece IS NULL\n" +
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
                "   'F_DOCREGL',\n" +
                "   GETDATE());\n" +
                "END CATCH";
    }
    public static void sendDataElement(Connection sqlCon, String path,String database,int unibase)
    {
        dbSource = database;
        loadFile( path, sqlCon,unibase);
        loadDeleteFile(path,sqlCon);
//        DocEntete.loadDeleteFile(path,sqlCon);
    }

    public static void loadFile(String path,Connection sqlCon,int unibase){
        String [] children = getFile(path,file);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children){
                disableTrigger(sqlCon,tableName);
                readOnFile(path, filename, tableName + "_DEST", sqlCon);
                executeQuery(sqlCon, updateTableDest("", "'DR_No'", tableName, tableName + "_DEST",filename,unibase));
                sendData(sqlCon, path, filename, insert(filename));
               // deleteTempTable(sqlCon, tableName+"_DEST");
                enableTrigger(sqlCon,tableName);
            }
        }
    }

    public static void loadDeleteFile(String path,Connection sqlCon) {
        String [] children = getFile(path,"deleteList"+file);
        disableTrigger(sqlCon,tableName);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children){
                String deleteDocEntete = deleteDocRegl();
                loadDeleteInfo(path,tableName,filename,sqlCon,deleteDocEntete);
            }
        }
        enableTrigger(sqlCon,tableName);
    }

    public static void getDataElement(Connection sqlCon, String path, String database, String time, JSONObject type)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DR_No,DatabaseSource");
        getData(sqlCon, selectSourceTable(tableName,database,type), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }
    public static void getDataElementFilterAgency(Connection sqlCon, String path,String database,String time,String agency)
    {
        String filename =  file+time+".csv";
        dbSource = database;
        initTableParam(sqlCon,tableName,configList,"DO_Domaine,DO_Type,DO_Piece,DR_No,DatabaseSource");
        getData(sqlCon, selectSourceTableFilterAgencyEnteteLink(tableName,database,agency), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);

    }

    public static String deleteDocRegl()
    {
        return
                " DELETE FROM F_DOCREGL \n" +
                " WHERE EXISTS (SELECT 1 FROM F_DOCREGL_SUPPR WHERE ISNULL(F_DOCREGL.DataBaseSource,'') = ISNULL(F_DOCREGL_SUPPR.DataBaseSource,'') AND ISNULL(F_DOCREGL.DR_NoSource,0) = ISNULL(F_DOCREGL_SUPPR.DR_No,0) ) \n" +
                " \n" +
                " IF OBJECT_ID('F_DOCENTETE_SUPPR') IS NOT NULL \n" +
                " DROP TABLE F_DOCENTETE_SUPPR \n";
    }

}
