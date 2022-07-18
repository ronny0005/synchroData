import java.io.File;
import java.io.FilenameFilter;
import java.sql.Connection;

public class ArtStock extends Table {

    public static String file = "ArtStock_";
    public static String tableName = "F_ARTSTOCK";
    public static String configList = "listArtStock";

    public static String insert(String filename)
    {
        return          "BEGIN TRY " +
                        "\n" +
                        "SET DATEFORMAT ymd;\n" +
                        " IF OBJECT_ID('F_ARTSTOCK_DEST') IS NOT NULL\n"+
                        "WITH GlobalTable AS (\n" +
                        "SELECT\tdest.*\n" +
                        "\t\t,DE_NoFinal =dep.DE_No \n" +
                        "\t\t,depempl.DP_No\n" +
                        "FROM\tF_ARTSTOCK_DEST dest\n" +
                        "INNER JOIN F_DEPOT dep \n" +
                        "\tON\tdep.DE_NoSource = dest.DE_No \n" +
                        "\tAND dep.DatabaseSource = dest.DatabaseSource  \n" +
                        "LEFT JOIN F_DEPOTEMPL depempl \n" +
                        "\tON\tdepempl.DP_NoSource = dest.DP_NoPrincipal \n" +
                        "\tAND depempl.DatabaseSource = dest.DatabaseSource   \n" +
                        ")\n" +
                        "UPDATE upd SET \n" +
                        "\t\t\tAS_QteMini = dest.AS_QteMini\n" +
                        "\t\t\t,AS_QteMaxi = dest.AS_QteMaxi\n" +
                        "\t\t\t,AS_MontSto = dest.AS_MontSto \n" +
                        "\t\t\t,AS_QteSto = dest.AS_QteSto\n" +
                        "\t\t\t,AS_QteRes = dest.AS_QteRes\n" +
                        "\t\t\t,AS_QteCom = dest.AS_QteCom\n" +
                        "\t\t\t,AS_Principal = dest.AS_Principal \n" +
                        "\t\t\t,AS_QteResCM = dest.AS_QteResCM\n" +
                        "\t\t\t,AS_QteComCM = dest.AS_QteComCM\n" +
                        "\t\t\t,AS_QtePrepa = dest.AS_QtePrepa\n" +
                        "\t\t\t,AS_QteAControler = dest.AS_QteAControler\n" +
                        "\t\t\t,DP_NoPrincipal = dest.DP_No\n" +
                        "\t\t\t,cbProt = dest.cbProt\n" +
                        "\t\t\t,cbModification = dest.cbModification\n" +
                        "\t\t\t,cbFlag = dest.cbFlag \n" +
                        "FROM F_ARTSTOCK upd\n" +
                        "INNER JOIN GlobalTable dest\n" +
                        "\tON\tupd.DE_No = dest.DE_NoFinal\n" +
                        "\tAND upd.AR_Ref = dest.AR_Ref; \n"+
                        "INSERT INTO [dbo].[F_ARTSTOCK]  \n" +
                        "([AR_Ref],[DE_No],[AS_QteMini],[AS_QteMaxi],[AS_MontSto],[AS_QteSto],[AS_QteRes],[AS_QteCom],[AS_Principal],[AS_QteResCM],[AS_QteComCM]\n" +
                        "      ,[AS_QtePrepa],[DP_NoPrincipal],[DP_NoControle],[AS_QteAControler],[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag])  \n" +
                        "                  \n" +
                        "SELECT\tdest.[AR_Ref]\n" +
                        "\t\t,dep.DE_No,[AS_QteMini],[AS_QteMaxi]\n" +
                        "\t\t,[AS_MontSto],[AS_QteSto],[AS_QteRes],[AS_QteCom],[AS_Principal],[AS_QteResCM],[AS_QteComCM]\n" +
                        "\t\t,[AS_QtePrepa]\n" +
                        "\t\t,[DP_NoPrincipal] = ISNULL(depempl.[DP_No],dest.[DP_NoPrincipal])\n" +
                        "\t\t,[DP_NoControle]\n" +
                        "\t\t,[AS_QteAControler],dest.[cbProt],dest.[cbCreateur]\n" +
                        "\t\t,dest.[cbModification],dest.[cbReplication],dest.[cbFlag] \n" +
                        "FROM F_ARTSTOCK_DEST dest    \n" +
                        "INNER JOIN F_DEPOT dep \n" +
                        "\tON\tdep.DE_NoSource = dest.DE_No \n" +
                        "\tAND dep.DatabaseSource = dest.DatabaseSource       \n" +
                        "LEFT JOIN F_DEPOTEMPL depempl \n" +
                        "\tON\tdepempl.DP_NoSource = dest.DP_NoPrincipal \n" +
                        "\tAND depempl.DatabaseSource = dest.DatabaseSource       \n" +
                        "WHERE NOT EXISTS (\tSELECT * \n" +
                        "\t\t\t\t\tFROM  F_ARTSTOCK a \n" +
                        "\t\t\t\t\tWHERE a.DE_No = dep.DE_No \n" +
                        "\t\t\t\t\tAND\t  a.AR_Ref = dest.AR_Ref);  \n" +
                        "IF OBJECT_ID('F_ARTSTOCK_DEST') IS NOT NULL  \n" +
                        "DROP TABLE F_ARTSTOCK_DEST;\n" +
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
                        "   'F_ARTSTOCK',\n" +
                        "   GETDATE());\n" +
                        "END CATCH";
    }

    public static void sendDataElement(Connection  sqlCon, String path,String database)
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
                //executeQuery(sqlCon, updateTableDest("AR_Ref,DE_No", "'AR_Ref','DE_No'", tableName, tableName + "_DEST"));
                sendData(sqlCon, path, filename, insert(filename));

                deleteTempTable(sqlCon, tableName + "_DEST");
                deleteArtStock(sqlCon, path, filename, database);
            }
        }
    }

    public static String selectSourceTable(String table,String dataSource){
        return "BEGIN \n" +
                "DECLARE @MaColonne AS VARCHAR(250);\n" +
                "DECLARE @MonSQL AS VARCHAR(MAX)=''; \n" +
                "DECLARE @TableName AS VARCHAR(100) = '"+table+"'; \n" +
                "DECLARE @getid CURSOR\n" +
                "\n" +
                "SET @getid = CURSOR FOR\n" +
                "SELECT col.name\n" +
                "FROM sys.tables tab\n" +
                "INNER JOIN sys.columns col\n" +
                "\tON tab.object_id = col.object_id\n" +
                "WHERE tab.name = @TableName\n" +
                "AND col.name NOT LIKE 'cb%'" +
                "AND col.name NOT IN ('DataBaseSource','cbMarqSource')\n" +
                "\n" +
                "OPEN @getid\n" +
                "FETCH NEXT\n" +
                "FROM @getid INTO @MaColonne\n" +
                "WHILE @@FETCH_STATUS = 0\n" +
                "BEGIN\n" +
                " SELECT @MonSQL = @MonSQL+ ',' + @MaColonne \n" +
                "\n" +
                " FETCH NEXT\n" +
                "    FROM @getid INTO @MaColonne --, @name\n" +
                "END\n" +
                "CLOSE @getid\n" +
                "DEALLOCATE @getid\n" +
                "SELECT @MonSQL = SUBSTRING(@MonSQL,2,LEN(@MonSQL)) \n" +
                "\n" +
                "SELECT @MonSQL = 'SELECT ' + @MonSQL + ',[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag],cbMarqSource = [cbMarq],[DataBaseSource] = ''"+dataSource+"''  FROM '+ @TableName " +
                "IF EXISTS (\tSELECT\tcol.name  \n" +
                "\t\t\tFROM\tsys.tables tab  \n" +
                "\t\t\tINNER JOIN sys.columns col\tON\ttab.object_id = col.object_id  \n" +
                "\t\t\tWHERE\ttab.name = @TableName  \n" +
                "\t\t\tAND\t\tcol.name = 'DataBaseSource') \n" +
                "\t SELECT @MonSQL = @MonSQL + ' AND ISNULL(DataBaseSource,''"+dataSource+"'') = ''"+dataSource+"''' \n" +
                "EXEC(@MonSQL)\n" +
                "\n" +
                "END";
    }

    public static void getDataElement(Connection  sqlCon, String path,String database,String time)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"AR_Ref,DE_No");
        getData(sqlCon, selectSourceTable(tableName,database), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

    public static void getDataElementFilterAgency(Connection  sqlCon, String path,String database,String time,String agency)
    {
        String filename =  file+time+".csv";
        initTableParam(sqlCon,tableName,configList,"AR_Ref,DE_No");
        getData(sqlCon, selectSourceTableFilterAgency(tableName,database,agency,"DE_No"), tableName, path, filename);
        listDeleteAllInfo(sqlCon, path, "deleteList" + filename,tableName,configList,database);
    }

    public static void deleteArtStock(Connection sqlCon, String path,String filename,String database)
    {
        String query =
                " DELETE FROM F_ARTSTOCK \n" +
                " WHERE EXISTS (SELECT 1 FROM F_ARTSTOCK_SUPPR artSuppr \n" +
                        "       INNER JOIN F_DEPOT dep \n" +
                        "       ON  artSuppr.DE_No = dep.DE_NoSource \n" +
                        "       AND dep.dataBaseSource = '" + database +"' \n" +
                        "       WHERE artSuppr.AR_Ref = F_ARTSTOCK.AR_Ref" +
                        "       AND dep.DE_No = F_ARTSTOCK.DE_No)  \n" +
                        "  \n" +
                        " IF OBJECT_ID('F_ARTSTOCK_SUPPR') IS NOT NULL  \n" +
                        " DROP TABLE F_ARTSTOCK_SUPPR ;";
        if ((new File(path + "\\deleteList" + filename)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + filename);
        }
    }
}
