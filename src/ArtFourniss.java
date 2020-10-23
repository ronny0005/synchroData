import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class ArtFourniss extends Table {

    public static String file ="ArtFourniss_";
    public static String tableName = "F_ARTFOURNISS";
    public static String configList = "listArtFourniss";

    public static String insert()
    {
        return
                        "  BEGIN TRY " +
                        " SET DATEFORMAT ymd;\n" +
                        "  INSERT INTO [dbo].[F_ARTFOURNISS]   \n" +
                        "  ([AR_Ref],[CT_Num],[AF_RefFourniss],[AF_PrixAch],[AF_Unite],[AF_Conversion],[AF_DelaiAppro]\n" +
                        "      ,[AF_Garantie],[AF_Colisage],[AF_QteMini],[AF_QteMont],[EG_Champ],[AF_Principal]\n" +
                        "      ,[AF_PrixDev],[AF_Devise],[AF_Remise],[AF_ConvDiv],[AF_TypeRem],[AF_CodeBarre]\n" +
                        "      ,[AF_PrixAchNouv],[AF_PrixDevNouv],[AF_RemiseNouv],[AF_DateApplication],[cbProt],[cbCreateur]\n" +
                        "      ,[cbModification],[cbReplication],[cbFlag])   \n" +
                        "   \n" +
                        "  SELECT dest.[AR_Ref],dest.[CT_Num],[AF_RefFourniss],[AF_PrixAch],[AF_Unite],[AF_Conversion],[AF_DelaiAppro]\n" +
                        "      ,[AF_Garantie],[AF_Colisage],[AF_QteMini],[AF_QteMont],[EG_Champ],[AF_Principal]\n" +
                        "      ,[AF_PrixDev],[AF_Devise],[AF_Remise],[AF_ConvDiv],[AF_TypeRem],[AF_CodeBarre]\n" +
                        "      ,[AF_PrixAchNouv],[AF_PrixDevNouv],[AF_RemiseNouv],[AF_DateApplication],[cbProt],[cbCreateur]\n" +
                        "      ,[cbModification],[cbReplication],[cbFlag] \n" +
                        "  FROM F_ARTFOURNISS_DEST dest     \n" +
                        "  LEFT JOIN (SELECT [AR_Ref],[CT_Num] FROM F_ARTFOURNISS) src     \n" +
                        "  ON dest.[AR_Ref] = src.[AR_Ref]     \n" +
                        "  AND dest.[CT_Num] = src.[CT_Num] \n" +
                        "  WHERE src.[AR_Ref] IS NULL ;   \n" +
                        "  IF OBJECT_ID('F_ARTFOURNISS_DEST') IS NOT NULL   \n" +
                        "  DROP TABLE F_ARTFOURNISS_DEST\n" +
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
                        "   'insert',\n" +
                        "   'F_ARTFOURNISS',\n" +
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
                executeQuery(sqlCon, updateTableDest("AR_Ref,CT_Num", "'AR_Ref','CT_Num'", tableName, tableName + "_DEST"));
                sendData(sqlCon, path, filename, insert());

                deleteTempTable(sqlCon, tableName);
                deleteArtFourniss(sqlCon, path,filename);
            }
        }
    }

    public static void getDataElement(Connection sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"AR_Ref,CT_Num");//initTable(sqlCon);
        getData(sqlCon, selectSourceTable(tableName,database)/*list()*/, tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

    public static void initTable(Connection sqlCon)
    {
        String query = " IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='F_ARTFOURNISS') \n" +
                "     INSERT INTO config.ListArtFourniss\n" +
                "     SELECT AR_Ref,CT_Num,cbMarq \n" +
                "     FROM F_ARTFOURNISS";
        executeQuery(sqlCon, query);
    }

    public static void deleteArtFourniss(Connection sqlCon, String path,String filename)
    {
        String query =
                " DELETE FROM F_ARTFOURNISS  \n" +
                " WHERE EXISTS (SELECT 1 FROM F_ARTFOURNISS_SUPPR WHERE F_ARTFOURNISS_SUPPR.AR_Ref = F_ARTFOURNISS.AR_Ref AND F_ARTFOURNISS_SUPPR.CT_Num = F_ARTFOURNISS.CT_Num" +
                "   )  \n" +
                "  \n" +
                " IF OBJECT_ID('F_ARTFOURNISS_SUPPR') IS NOT NULL  \n" +
                " DROP TABLE F_ARTFOURNISS_SUPPR ;";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }

}
