import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.json.simple.JSONObject;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.util.List;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class Table {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.s";
    private static Statement stmt;

    public static String selectSourceTable(String table,String dataSource,boolean existsCbModification){
        return "BEGIN \n" +
                "\nDECLARE @TableName AS VARCHAR(100) = '"+table+"'; \n" +
                "\nDECLARE @cbModification VARCHAR(100) = "+ ((!existsCbModification) ? "NULL" : "ISNULL((SELECT CONVERT(NVARCHAR(100),(SELECT MAX(cbModification) FROM " + table + "),25)),'1901-01-01');\n") +
                "\nDECLARE @lastSynchro VARCHAR(100) = ISNULL((SELECT CONVERT(NVARCHAR(100),(SELECT CASE WHEN ISNULL(isLoaded,0) = 2 THEN lastSynchro ELSE DATEADD(HOUR,-1,lastSynchro) END FROM config.SelectTable WHERE tableName = @TableName),25)),'1901-01-01');" +

                "IF EXISTS(SELECT 1 FROM config.SelectTable WHERE tableName = @TableName) \n" +
                " UPDATE config.SelectTable " +
                "     SET lastSynchro =  CONVERT(DATETIME,@cbModification,20) \n" +
                "     , isLoaded =  CASE WHEN CONVERT(DATETIME,@cbModification,20) = lastSynchro THEN " +
                "                               CASE WHEN ISNULL(isLoaded,0) < 2 THEN  ISNULL(isLoaded,0) + 1 ELSE  ISNULL(isLoaded,0) END" +
                "                       ELSE 0 END\n" +
                " WHERE tableName = @TableName; \n" +
                "         ELSE \n" +
                "             INSERT INTO config.SelectTable(tableName,lastSynchro,isLoaded) VALUES(@TableName,CONVERT(DATETIME,@cbModification,20),0); \n"+
                "DECLARE @MaColonne AS VARCHAR(250);\n" +
                "DECLARE @MonSQL AS VARCHAR(MAX)=''; \n" +
                "DECLARE @databaseSource AS VARCHAR(150) = '"+dataSource+"'; \n" +
                "DECLARE @getid CURSOR\n" +
                "\n" +
                "SET @getid = CURSOR FOR\n" +
                "SELECT col.name\n" +
                "FROM sys.tables tab\n" +
                "INNER JOIN sys.columns col\n" +
                "\tON tab.object_id = col.object_id\n" +
                "WHERE tab.name = @TableName\n" +
                "AND (col.name NOT LIKE 'cb%' OR col.Name = 'cbIndice')" +
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
                "SELECT @MonSQL = 'DECLARE @databaseSource AS VARCHAR (150) = '''+@databaseSource+'''; SELECT ' + @MonSQL \n" +
                "+ CASE WHEN @cbModification IS NOT NULL THEN ',[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag]' ELSE '' END +',cbMarqSource = [cbMarq],[DataBaseSource] = @databaseSource  FROM '\n" +
                "+ @TableName +  CASE WHEN @cbModification IS NOT NULL THEN ' WHERE cbModification > CONVERT(DATETIME,''' + @lastSynchro +''',20)' ELSE '' END\n" +
                "IF EXISTS (\tSELECT\tcol.name  \n" +
                "\t\t\tFROM\tsys.tables tab  \n" +
                "\t\t\tINNER JOIN sys.columns col\tON\ttab.object_id = col.object_id  \n" +
                "\t\t\tWHERE\ttab.name = @TableName  \n" +
                "\t\t\tAND\t\tcol.name = 'DataBaseSource') \n" +
                "\t SELECT @MonSQL = @MonSQL + ' AND ISNULL(DataBaseSource,''' + @databaseSource +''') = ''' + @databaseSource +''''\n" +
                "EXEC(@MonSQL)\n" +
                "\n" +
                "END";
    }

    public static String selectSourceTable(String table, String dataSource, JSONObject type){
        return "BEGIN \n" +
                "DECLARE @TableName AS VARCHAR(100) = '"+table+"'; \n" +
                "DECLARE @cbModification VARCHAR(100) = ISNULL((SELECT CONVERT(NVARCHAR(100),(SELECT MAX(cbModification) FROM " + table + "),25)),'1901-01-01');" +
                "DECLARE @lastSynchro VARCHAR(100) = ISNULL((SELECT CONVERT(NVARCHAR(100),(SELECT CASE WHEN ISNULL(isLoaded,0) = 2 THEN lastSynchro ELSE DATEADD(HOUR,-1,lastSynchro) END FROM config.SelectTable WHERE tableName = @TableName),25)),'1901-01-01');" +

                "IF EXISTS(SELECT 1 FROM config.SelectTable WHERE tableName = @TableName) \n" +
                " UPDATE config.SelectTable " +
                "     SET lastSynchro =  CONVERT(DATETIME,@cbModification,20) \n" +
                "     , isLoaded =  CASE WHEN CONVERT(DATETIME,@cbModification,20) = lastSynchro THEN " +
                "                               CASE WHEN  ISNULL(isLoaded,0) < 2 THEN  ISNULL(isLoaded,0) + 1 ELSE  ISNULL(isLoaded,0) END" +
                "                       ELSE 0 END\n" +
                " WHERE tableName = @TableName; \n" +
                "         ELSE \n" +
                "             INSERT INTO config.SelectTable(tableName,lastSynchro,isLoaded) VALUES(@TableName,CONVERT(DATETIME,@cbModification,20),0); \n"+
                "DECLARE @MaColonne AS VARCHAR(250);\n" +
                "DECLARE @MonSQL AS VARCHAR(MAX)=''; \n" +
                "DECLARE @facturevente AS VARCHAR(1) = "+ type.get("facturedevente") +"; \n" +
                "DECLARE @vente AS VARCHAR(1) = "+ type.get("vente") +"; \n" +
                "DECLARE @stock AS VARCHAR(1) = "+ type.get("stock") +"; \n" +
                "DECLARE @devis AS VARCHAR(1) = "+type.get("devis")+"; \n" +
                "DECLARE @bonlivraison AS VARCHAR(1) = "+type.get("bondelivraison")+"; \n" +
                "DECLARE @factureachat AS VARCHAR(1) = "+type.get("facturedachat")+"; \n" +
                "DECLARE @entree AS VARCHAR(1) = "+type.get("entree")+"; \n" +
                "DECLARE @sortie AS VARCHAR(1) = "+type.get("sortie")+"; \n" +
                "DECLARE @transfert AS VARCHAR(1) = "+type.get("transfert")+"; \n" +
                "DECLARE @interne1 AS VARCHAR(1) = "+type.get("documentinterne1")+"; \n" +
                "DECLARE @interne2 AS VARCHAR(1) = "+type.get("documentinterne2")+"; \n" +
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
                "SELECT @MonSQL = 'SELECT ' + @MonSQL + ',[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag],cbMarqSource = [cbMarq],[DataBaseSource] = ''"+dataSource+"''  FROM '+ @TableName +'" +
                "  WHERE cbModification > CONVERT(DATETIME,''' + @lastSynchro +''',20) \n" +
                "  AND CASE WHEN ' + @vente + ' = 1 AND DO_Domaine = 0 THEN 1 \n" +
                "           WHEN ' + @facturevente + ' = 1 AND DO_Domaine = 0 AND DO_Type IN (6,7) THEN 1 \n" +
                "           WHEN ' + @devis + ' = 1 AND DO_Domaine = 0 AND DO_Type = 0 THEN 1 \n" +
                "           WHEN ' + @bonlivraison + ' = 1 AND DO_Domaine = 0 AND DO_Type = 3 THEN 1 \n" +
                "           WHEN ' + @factureachat + ' = 1 AND DO_Domaine = 1 THEN 1 \n" +
                "           WHEN ' + @stock + ' = 1 AND DO_Domaine = 2 THEN 1 \n" +
                "           WHEN ' + @entree + ' = 1 AND DO_Domaine = 2 AND DO_Type = 20 THEN 1 \n" +
                "           WHEN ' + @sortie + ' = 1 AND DO_Domaine = 2 AND DO_Type = 21 THEN 1 \n" +
                "           WHEN ' + @transfert + ' = 1 AND DO_Domaine = 2 AND DO_Type = 23 THEN 1 \n" +
                "           WHEN ' + @interne1 + ' = 1 AND DO_Domaine = 4 AND DO_Type = 40 THEN 1 \n" +
                "           WHEN ' + @interne2 + ' = 1 AND DO_Domaine = 4 AND DO_Type = 41 THEN 1 END = 1 '\n" +
                "\n" +
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

    public static String selectSourceTableFilterAgency(String table,String dataSource,String agency,String agencyColumn){
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
                "+ ' WHERE  cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='''+ @TableName +'''),''1900-01-01'')' " +
                "+ ' AND    "+agencyColumn+" IN (SELECT DE_No FROM F_DEPOT WHERE DE_CodePostal = ''"+agency+"'')'   \n" +
                "\n" +
                "EXEC(@MonSQL)\n" +
                "\n" +
                "END";
    }

    public static String selectSourceTableFilterAgencyArticle(String table,String dataSource,String agency){
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
                "AND CASE WHEN col.name LIKE 'cb%' THEN 0 " +
                " WHEN (col.name LIKE '%_P_MIN' AND LEFT(col.name ,LEN('"+agency+"')) <> '"+agency+"') THEN 0 \n " +
                " WHEN (col.name LIKE '%_QTE_MIN_DG' AND LEFT(col.name ,LEN('"+agency+"')) <>  '"+agency+"') THEN 0 \n " +
                " WHEN (col.name LIKE '%_P_MAX' AND LEFT(col.name ,LEN('"+agency+"')) <>  '"+agency+"') THEN 0 \n " +
                " WHEN (col.name LIKE '%_P_GROSSISTES' AND LEFT(col.name ,LEN('"+agency+"')) <>  '"+agency+"') THEN 0 \n " +
                " WHEN (col.name LIKE '%_QTE_MIN_G' AND LEFT(col.name ,LEN('"+agency+"')) <>  '"+agency+"') THEN 0 \n " +
                " WHEN (col.name LIKE '%_SUPER_PRIX' AND LEFT(col.name ,LEN('"+agency+"')) <>  '"+agency+"') THEN 0 \n " +
                " WHEN (col.name LIKE '%_QTE_MIN_SUPERPRIX' AND LEFT(col.name ,LEN('"+agency+"')) <>  '"+agency+"') THEN 0 \n " +
                " WHEN (col.name LIKE '%_COMMENTAIRES' AND LEFT(col.name ,LEN('"+agency+"')) <>  '"+agency+"') THEN 0 \n " +
                " WHEN col.name IN ('DataBaseSource','cbMarqSource') THEN 0 \n" +
                " ELSE 1 END = 1 " +
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
                "+ ' WHERE  cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='''+ @TableName +'''),''1900-01-01'')' " +
                "+ ''   \n" +
                "\n" +
                "EXEC(@MonSQL)\n" +
                "\n" +
                "END";
    }

    public static String selectSourceTableFilterAgencyEnteteLink(String table,String dataSource,String agency){
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
                "SELECT @MonSQL = 'SELECT ' + @MonSQL + ',[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag],cbMarqSource = [cbMarq],[DataBaseSource] = ''"+dataSource+
                "''  FROM '+ @TableName " +
                "+ ' WHERE  cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='''+ @TableName +'''),''1900-01-01'')' " +
                "+ ' AND    EXISTS (SELECT  F_DOCENTETE.DO_Domaine,F_DOCENTETE.DO_Piece,F_DOCENTETE.DO_Type " +
                "                   FROM    F_DOCENTETE " +
                "                   WHERE   DE_No IN (SELECT DE_No FROM F_DEPOT WHERE DE_CodePostal = ''"+agency+"'')" +
                "                   AND '+ @TableName +'.DO_Domaine = F_DOCENTETE.DO_Domaine " +
                "                   AND '+ @TableName +'.DO_Type = F_DOCENTETE.DO_Type" +
                "                   AND '+ @TableName +'.DO_Piece = F_DOCENTETE.DO_Piece)'   \n" +
                "\n" +
                "EXEC(@MonSQL)\n" +
                "\n" +
                "END";
    }

    public static String selectSourceTableFilterAgencyRegltLink(String table,String dataSource,String agency){
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
                "SELECT @MonSQL = 'SELECT ' + @MonSQL + ',[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag],cbMarqSource = [cbMarq],[DataBaseSource] = ''"+dataSource+
                "''  FROM '+ @TableName " +
                "+ ' WHERE  cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='''+ @TableName +'''),''1900-01-01'')' " +
                "+ ' AND    EXISTS (SELECT  F_DOCENTETE.DO_Domaine,F_DOCENTETE.DO_Piece,F_DOCENTETE.DO_Type " +
                "                   FROM    F_DOCENTETE" +
                "                   INNER JOIN F_REGLECH" +
                "                       ON  F_REGLECH.DO_Domaine = F_DOCENTETE.DO_Domaine" +
                "                       AND F_REGLECH.DO_Type = F_DOCENTETE.DO_Type" +
                "                       AND F_REGLECH.DO_Piece = F_DOCENTETE.DO_Piece    " +
                "                   INNER JOIN F_CREGLEMENT " +
                "                       ON  F_REGLECH.RG_No = F_CREGLEMENT.RG_No" +
                "                   WHERE   DE_No IN (SELECT DE_No FROM F_DEPOT WHERE DE_CodePostal = ''"+agency+"'')" +
                "                   AND '+ @TableName +'.RG_No = F_CREGLEMENT.RG_No )'   \n" +
                "\n" +
                "EXEC(@MonSQL)\n" +
                "\n" +
                "END";
    }

    public static void listDeleteAllInfo(Connection sqlCon, String path, String file,String table,String listTable,String database)
    {
        String query = listDelete(table,listTable,database);
        writeToFileAvro(path + "\\" + file, query, sqlCon);
        writeOnFile(path + "\\" + file, query, sqlCon);

        query = listDeleteItem(table,listTable);
        executeQuery(sqlCon, query);
    }

    public static String listDeleteItem(String table,String listTable){
        return " DELETE FROM config."+listTable+" " +
                " WHERE NOT EXISTS(SELECT 1 " +
                "                  FROM "+table +
                "                  WHERE dbo."+table+".cbMarq = config."+listTable+".cbMarq);";
    }

    public static void deleteTempTable(Connection sqlCon,String table)
    {
        String query = "IF OBJECT_ID('"+table+"') IS NOT NULL \n" +
                "\tDROP TABLE "+table+";";
        executeQuery(sqlCon, query);
    }


    public static void loadDeleteFile(String path,Connection sqlCon,String file,String tableName,String query) {
        File dir = new File(path);
        FilenameFilter filter = (dir1, name) -> name.startsWith("deleteList"+file);
        String [] children = dir.list(filter);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                loadDeleteInfo(path, tableName, filename, sqlCon, query);
            }
        }
    }

    public static String [] getFile(String path,String file){
        File dir = new File(path);
        FilenameFilter filter = (dir1, name) -> name.startsWith(file) ;
        return dir.list(filter);
    }

    public static void loadDeleteInfo(String path,String tableName,String filename,Connection sqlCon,String query){
        readOnFile(path, filename, tableName + "_SUPPR", sqlCon);
        File file = new File(path + "\\" + filename);
        if (file.exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, filename);
        }
        deleteTempTable(sqlCon, tableName+"_SUPPR");
    }

    public static void deleteItem(Connection sqlCon, String path,String file)
    {
        String query =
                "BEGIN TRY\n" +
                        "DECLARE @MaColonne AS VARCHAR(250);\n" +
                        "DECLARE @MonSQL AS VARCHAR(MAX)=''; \n" +
                        "DECLARE @TableName AS VARCHAR(100) = 'F_ARTICLE';\n" +
                        "DECLARE @isDataSource AS INT = 1; \n" +
                        "\n" +
                        "SELECT @MonSQL = 'DELETE FROM '+@TableName+' WHERE EXISTS (SELECT 1 FROM '+@TableName+'_SUPPR WHERE '\n" +
                        "+@TableName+'.cbMarqSource = '+@TableName+'_SUPPR.cbMarq'\n" +
                        "\n" +
                        "IF @isDataSource = 1\n" +
                        "\tSELECT @MonSQL = @MonSQL + ' AND '+@TableName+'.DataBaseSource = '+@TableName+'_SUPPR.DataBaseSource)'\n" +
                        "\n" +
                        "SELECT @MonSQL = @MonSQL + 'IF OBJECT_ID('''+@TableName+'_SUPPR'') IS NOT NULL DROP TABLE '+@TableName+'_SUPPR'\n" +
                        "\n" +
                        "SELECT (@MonSQL)\n" +
                        "\n" +
                        "END TRY\n" +
                        "BEGIN CATCH \n" +
                        "\tINSERT INTO config.DB_Errors\n" +
                        "\tVALUES\n" +
                        "\t(\n" +
                        "\t\tSUSER_SNAME(),\n" +
                        "\t\tERROR_NUMBER(),\n" +
                        "\t\tERROR_STATE(),\n" +
                        "\t\tERROR_SEVERITY(),\n" +
                        "\t\tERROR_LINE(),\n" +
                        "\t\tERROR_PROCEDURE(),\n" +
                        "\t\tERROR_MESSAGE(),\n" +
                        "\t\t@TableName,\n" +
                        "\t\t@MonSQL,\n" +
                        "\t\tGETDATE()\n" +
                        "\t);\n" +
                        "END CATCH\n";
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }

    public static String listDelete(String table,String listTable,String database){
        return  "BEGIN TRY\n" +
                "DECLARE @MaColonne AS VARCHAR(250);\n" +
                "DECLARE @MonSQL AS VARCHAR(MAX)=''; \n" +
                "DECLARE @TableName AS VARCHAR(100) = '"+table+"';\n" +
                "DECLARE @TableConfig AS VARCHAR(100) = '"+listTable+"'; \n" +
                "DECLARE @getid CURSOR\n" +
                "\n" +
                "SET @getid = CURSOR FOR\n" +
                "SELECT col.name\n" +
                "FROM sys.tables tab\n" +
                "INNER JOIN sys.columns col\n" +
                "\tON tab.object_id = col.object_id\n" +
                "WHERE tab.name = @TableConfig\n" +
                "\n" +
                "OPEN @getid\n" +
                "FETCH NEXT\n" +
                "FROM @getid INTO @MaColonne\n" +
                "WHILE @@FETCH_STATUS = 0\n" +
                "BEGIN\n" +
                " IF @MaColonne ='DataBaseSource' \n"+
                " SELECT @MonSQL = @MonSQL+ ',DataBaseSource = ''"+database+"'' ' \n" +
                " ELSE \n" +
                "   SELECT @MonSQL = @MonSQL+ ',lart.' + @MaColonne \n" +
                " FETCH NEXT\n" +
                "    FROM @getid INTO @MaColonne \n" +
                "END\n" +
                "CLOSE @getid\n" +
                "DEALLOCATE @getid\n" +
                "SELECT @MonSQL = 'SELECT '+ SUBSTRING(@MonSQL,2,LEN(@MonSQL))+' FROM config.'+@TableConfig\n" +
                "+' lart LEFT JOIN '+@TableName+ ' fart ON lart.cbMarq = fart.cbMarq WHERE fart.cbMarq IS NULL ';\n" +
                "\n" +
                "EXEC (@MonSQL)\n" +
                "\n" +
                "END TRY\n" +
                "BEGIN CATCH \n" +
                "\tINSERT INTO config.DB_Errors\n" +
                "\tVALUES\n" +
                "\t(\n" +
                "\t\tSUSER_SNAME(),\n" +
                "\t\tERROR_NUMBER(),\n" +
                "\t\tERROR_STATE(),\n" +
                "\t\tERROR_SEVERITY(),\n" +
                "\t\tERROR_LINE(),\n" +
                "\t\tERROR_PROCEDURE(),\n" +
                "\t\tERROR_MESSAGE(),\n" +
                "\t\t@TableName,\n" +
                "\t\t@MonSQL,\n" +
                "\t\tGETDATE()\n" +
                "\t);\n" +
                "END CATCH\n";
    }
    public static String updateTableDest(String key,String exclude,String tableName,String tableNameDest,String filename,int unibase){
        String[] keys = key.split(",");
        StringBuilder sql = new StringBuilder("\n" +
                "BEGIN TRY\n" +
                "SET DATEFORMAT ymd;\n" +
                "DECLARE @MaColonne AS VARCHAR(250);\n" +
                "DECLARE @MonSQL AS VARCHAR(MAX) = ''; \n" +
                "DECLARE @UniBase AS INT = "+unibase+"; \n" +
                "DECLARE @isKey AS INT=CASE WHEN '" + key + "' = '' THEN 0 ELSE 1 END; \n" +
                "DECLARE @Key AS NVARCHAR(250) = '" + key + "'; \n" +
                "DECLARE @TableName AS VARCHAR(100) = '" + tableName + "'; \n" +
                "DECLARE @TableNameDest AS VARCHAR(100) = '" + tableNameDest + "'; \n" +
                "DECLARE @getid CURSOR\n" +
                "\n" +
                "SET @getid = CURSOR FOR\n" +
                "SELECT col.name\n" +
                "FROM sys.tables tab\n" +
                "INNER JOIN sys.columns col\n" +
                "\tON tab.object_id = col.object_id\n" +
                "WHERE tab.name = @TableName\n" +
                "AND col.name NOT LIKE 'cb%'\n" +
                "AND col.name NOT IN (" + exclude + ")\n" +
                "\n" +
                "OPEN @getid\n" +
                "FETCH NEXT\n" +
                "FROM @getid INTO @MaColonne\n" +
                "WHILE @@FETCH_STATUS = 0\n" +
                "BEGIN\n" +
                " SELECT @MonSQL = @MonSQL+ ',' + @MaColonne +' = ' + @TableNameDest + '.' + @MaColonne \n" +
                " FETCH NEXT\n" +
                "    FROM @getid INTO @MaColonne \n" +
                "END\n" +
                "CLOSE @getid\n" +
                "DEALLOCATE @getid\n" +
                " SELECT @MonSQL = 'IF OBJECT_ID('''+@TableNameDest+''') IS NOT NULL UPDATE '+@TableName+' SET '+ SUBSTRING(@MonSQL,2,LEN(@MonSQL));\n" +
                " SELECT @MonSQL = @MonSQL + ' FROM ' + @TableNameDest  +' WHERE '\n" +
                "IF @isKey = 0 \n" +
                "\tSELECT @MonSQL = @MonSQL + @TableName + '.cbMarqSource = '+ @TableNameDest+'.cbMarqSource '\n" +
                "\t\t\t\t\t\t+' AND CASE WHEN '+CAST(@UniBase AS VARCHAR(150))+'= 0 AND '+@TableName + '.DataBaseSource = '+ @TableNameDest+'.DataBaseSource THEN 1'\n" +
                "\t\t\t\t\t\t+'             WHEN '+CAST(@UniBase AS VARCHAR(150))+'= 1 THEN 1 ELSE 0 END = 1'\n" +
                "ELSE \n");
        for (int i = 0;i< keys.length;i++) {
            if(i== 0 ) {
                sql.append(" SELECT @MonSQL = @MonSQL ");
                sql.append("+ @TableName + '.").append(keys[i]).append(" = '+ @TableNameDest+'.").append(keys[i]).append(" '");
            }
            if(i>0)
                sql.append("+' AND '+@TableName + '.").append(keys[i]).append(" = '+ @TableNameDest+'.").append(keys[i]).append(" '");
        }
        sql.append("\n").append("EXEC (@MonSQL) \n").append("\n").append("END TRY\n").append("BEGIN CATCH \n").append("\tINSERT INTO config.DB_Errors\n").append("\tVALUES\n").append("\t(\n").append("\t\tSUSER_SNAME(),\n").append("\t\tERROR_NUMBER(),\n").append("\t\tERROR_STATE(),\n").append("\t\tERROR_SEVERITY(),\n").append("\t\tERROR_LINE(),\n").append("\t\tERROR_PROCEDURE(),\n").append("\t\tERROR_MESSAGE(),\n").append("\t\t@TableName + ' ").append(filename).append("',\n").append("\t\t@MonSQL,\n").append("\t\tGETDATE()\n").append("\t);\n").append("END CATCH");
        return sql.toString();
    }
    public static void archiveDocument(String archive, String source,String file)
    {
        String[] folder = file.split("_");
        String year = folder[1].substring(0,4);
        String month = folder[1].substring(4,6);
        String day = folder[1].substring(6,8);

        File filePath = new File(archive);
        if(!filePath.exists())
            filePath.mkdir();

        filePath = new File(archive + "\\" + year );
        if(!filePath.exists())
            filePath.mkdir();

        filePath = new File(archive + "\\" + year + "\\" + month);
        if(!filePath.exists())
            filePath.mkdir();

        filePath = new File(archive + "\\" + year + "\\" + month +"\\"+ day);
        if(!filePath.exists())
            filePath.mkdir();

        archive = archive + "\\" + year + "\\" + month +"\\"+ day;
        try {
            Files.move(Paths.get(source+"\\"+file),Paths.get(archive + "\\" + file),REPLACE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeOnDb(Connection sqlCon, String path, String fileName, String query)
    {

        File filePath = new File(path + "\\" + fileName);
        if (filePath.exists())
        {
            executeQuery(sqlCon,query);
            archiveDocument(path + "\\archive", path, fileName);
        }
    }

    public static void sendData(Connection sqlCon, String path,String file,String query)
    {
        writeOnDb(sqlCon, path, file, query);
    }


    public static boolean isDateValid(String date)
    {
        try {
            DateFormat df = new SimpleDateFormat(DATE_FORMAT);
            df.setLenient(false);
            df.parse(date);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    // Fonction de mappage SQL -> Avro
    private static String mapSQLTypeToAvroType(String sqlType) {
        switch (sqlType.toLowerCase()) {
            case "int":
            case "integer":
                return "[\"null\", \"int\"]";
            case "bigint":
                return "[\"null\", \"long\"]";
            case "float":
            case "real":
                return "[\"null\", \"float\"]";
            case "double":
            case "numeric":
            case "decimal":
                return "[\"null\", \"double\"]";
            case "bit":
            case "boolean":
                return "[\"null\", \"boolean\"]";
            case "char":
            case "varchar":
            case "nvarchar":
            case "text":
                return "[\"null\", \"string\"]";
            case "date":
            case "time":
            case "timestamp":
                return "[\"null\", {\"type\": \"string\", \"logicalType\": \"timestamp-millis\"}]";
            case "smallint":
                return "[\"null\", \"int\"]";
            case "tinyint":
                return "[\"null\", \"int\"]";
            default:
                return "[\"null\", \"string\"]";
        }
    }


    // Méthode principale pour écrire le résultat de la requête dans un fichier Avro
    public static void writeToFileAvro(String fileName, String query, Connection sqlCon) {
        try (Statement statement = sqlCon.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            // Récupérer les métadonnées pour construire le schéma Avro
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            StringBuilder schemaBuilder = new StringBuilder();
            schemaBuilder.append("{\"type\":\"record\",\"name\":\"ResultSetRecord\",\"fields\":[");

            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                String columnType = mapSQLTypeToAvroType(metaData.getColumnTypeName(i));
                schemaBuilder.append("{\"name\":\"").append(columnName)
                        .append("\",\"type\":").append(columnType)
                        .append("}");
                if (i < columnCount) {
                    schemaBuilder.append(",");
                }
            }
            schemaBuilder.append("]}");

            org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(schemaBuilder.toString());

            // Créer un Avro DataFileWriter
            File file = new File(fileName.replace(".csv",".avro"));
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>());
            dataFileWriter.create(schema, file);

            // Ecrire les résultats dans le fichier Avro
            while (resultSet.next()) {
                GenericRecord record = new GenericData.Record(schema);
                for (int i = 1; i <= columnCount; i++) {
                    Object value = resultSet.getObject(i);
                    String columnName = metaData.getColumnName(i);

                    if (value == null) {
                        record.put(columnName, null);
                    } else {
                        // Récupération du type Avro attendu pour cette colonne
                        String avroType = schema.getField(columnName).schema().getTypes().get(1).getType().getName();

                        switch (avroType) {
                            case "int":
                                record.put(columnName, ((Number) value).intValue());
                                break;
                            case "long":
                                record.put(columnName, ((Number) value).longValue());
                                break;
                            case "float":
                                record.put(columnName, ((Number) value).floatValue());
                                break;
                            case "double":
                                record.put(columnName, ((Number) value).doubleValue());
                                break;
                            case "boolean":
                                record.put(columnName, value);
                                break;
                            case "string":
                                record.put(columnName, value.toString());
                                break;
                            default:
                                record.put(columnName, value.toString());
                                break;
                        }
                    }
                }
                dataFileWriter.append(record);
            }

            // Fermer le writer
            dataFileWriter.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void writeOnFile(String fileName, String query, Connection sqlCon)
    {
        ResultSet result;
        FileWriter fileWriter;
        try {
            result = executeQueryResult(sqlCon,query);

            if(result.isBeforeFirst()) {          //res.isBeforeFirst() is true if the cursor
                fileWriter = new FileWriter(fileName, StandardCharsets.ISO_8859_1);
                int columnCount = writeHeaderLine(result, fileWriter);
                while (result.next()) {
                    String line = "";

                    for (int i = 1; i <= columnCount; i++) {
                        Object valueObject = result.getObject(i);
                        String valueString = "";

                        if (valueObject != null) valueString = valueObject.toString();

                        if (valueObject instanceof String) {
                            valueString = "\"" + escapeDoubleQuotes(valueString) + "\"";
                        }

                        if (isDateValid(valueString)) {
                            valueString = "\"" + valueString.replace(".0", "") + "\"";
                        }

                        line = line.concat(valueString);

                        if (i != columnCount) {
                            line = line.concat(";");
                        }
                    }

                    fileWriter.write(line + "\n");
                    fileWriter.flush();
                }
                fileWriter.close();
                stmt.close();
            }
        } catch (SQLException | IOException throwables) {
            throwables.printStackTrace();
        }
    }

    // Method to map Avro types to SQL Server types
    private static String mapAvroTypeToSQLType(org.apache.avro.Schema schema) {
        org.apache.avro.Schema.Type avroType = schema.getType();
        if (avroType == org.apache.avro.Schema.Type.UNION) {
            // If it's a union, find the non-null type
            List<org.apache.avro.Schema> types = schema.getTypes();
            for (org.apache.avro.Schema type : types) {
                if (!type.getType().equals(org.apache.avro.Schema.Type.NULL)) {
                    avroType = type.getType();
                    break;
                }
            }
        }

        switch (avroType) {
            case INT:
                return "INT";
            case LONG:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "FLOAT";
            case BOOLEAN:
                return "BIT";
            case STRING:
                return "NVARCHAR(MAX)";
            case BYTES:
                return "VARBINARY(MAX)";
            default:
                return "NVARCHAR(MAX)";
        }
    }


    // Lire le fichier Avro et insérer les données dans la base de données
    public static void insertAvroDataToSqlServer(String avroFilePath, String tableName, Connection conn) {
        File avroFile = new File(avroFilePath);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();

        try (org.apache.avro.file.FileReader<GenericRecord> dataFileReader = DataFileReader.openReader(avroFile, datumReader)) {
            // Get Avro schema
            org.apache.avro.Schema schema = dataFileReader.getSchema();

            // Create table dynamically
            createTableFromSchema(conn, schema, tableName);

            // Prepare insert query
            String sqlInsertQuery = generateInsertQuery(schema, tableName);
            try (PreparedStatement preparedStatement = conn.prepareStatement(sqlInsertQuery)) {

                // Read each Avro record and insert into the database
                while (dataFileReader.hasNext()) {
                    GenericRecord record = dataFileReader.next();
                    setPreparedStatementParameters(preparedStatement, record);
                    preparedStatement.executeUpdate();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Method to create a SQL Server table from Avro schema
    private static void createTableFromSchema(Connection conn, org.apache.avro.Schema schema, String tableName) throws SQLException {
        StringBuilder createTableQuery = new StringBuilder("DROP TABLE IF EXISTS ").append(tableName).append("; CREATE TABLE ");
        createTableQuery.append(tableName).append(" (");

        for (org.apache.avro.Schema.Field field : schema.getFields()) {
            createTableQuery.append(field.name()).append(" ");
            createTableQuery.append(mapAvroTypeToSQLType(field.schema())).append(",");
        }

        // Remove the last comma
        createTableQuery.setLength(createTableQuery.length() - 1);
        createTableQuery.append(");");

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableQuery.toString());
        }
    }

    // Method to generate the SQL insert query from Avro schema
    private static String generateInsertQuery(org.apache.avro.Schema schema, String tableName) {
        StringBuilder insertQuery = new StringBuilder("INSERT INTO ");
        insertQuery.append(tableName).append(" (");

        StringBuilder valuesPart = new StringBuilder(" VALUES (");

        for (org.apache.avro.Schema.Field field : schema.getFields()) {
            insertQuery.append(field.name()).append(",");
            valuesPart.append("?,");
        }

        // Remove the last comma
        insertQuery.setLength(insertQuery.length() - 1);
        valuesPart.setLength(valuesPart.length() - 1);

        insertQuery.append(") ").append(valuesPart).append(");");
        return insertQuery.toString();
    }

    // Method to set parameters for PreparedStatement from GenericRecord
    private static void setPreparedStatementParameters(PreparedStatement preparedStatement, GenericRecord record) throws SQLException {
        int index = 1;
        for (org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
            Object value = record.get(field.name());
            org.apache.avro.Schema fieldSchema = field.schema();
            org.apache.avro.Schema.Type fieldType = fieldSchema.getType();

            // Handle union types by finding the non-null type
            if (fieldType == org.apache.avro.Schema.Type.UNION) {
                List<org.apache.avro.Schema> types = fieldSchema.getTypes();
                for (org.apache.avro.Schema type : types) {
                    if (!type.getType().equals(org.apache.avro.Schema.Type.NULL)) {
                        fieldType = type.getType();
                        break;
                    }
                }
            }

            if (value == null) {
                preparedStatement.setNull(index, getSqlType(fieldType));
            } else {
                switch (fieldType) {
                    case INT:
                        preparedStatement.setInt(index, (Integer) value);
                        break;
                    case LONG:
                        preparedStatement.setLong(index, (Long) value);
                        break;
                    case FLOAT:
                        preparedStatement.setFloat(index, (Float) value);
                        break;
                    case DOUBLE:
                        preparedStatement.setDouble(index, (Double) value);
                        break;
                    case BOOLEAN:
                        preparedStatement.setBoolean(index, (Boolean) value);
                        break;
                    case STRING:
                        preparedStatement.setString(index, value.toString());
                        break;
                    case BYTES:
                        preparedStatement.setBytes(index, (byte[]) value);
                        break;
                    default:
                        preparedStatement.setObject(index, value.toString());
                        break;
                }
            }
            index++;
        }
    }

    // Helper method to get SQL type from Avro schema type
    private static int getSqlType(org.apache.avro.Schema.Type avroType) {
        switch (avroType) {
            case INT:
                return java.sql.Types.INTEGER;
            case LONG:
                return java.sql.Types.BIGINT;
            case FLOAT:
                return java.sql.Types.FLOAT;
            case DOUBLE:
                return java.sql.Types.DOUBLE;
            case BOOLEAN:
                return java.sql.Types.BIT;
            case STRING:
                return java.sql.Types.NVARCHAR;
            case BYTES:
                return java.sql.Types.VARBINARY;
            default:
                return java.sql.Types.VARCHAR;
        }
    }

    public static void readOnFile(String path,String fileInfo,String table,Connection sqlCon)
    {

        insertAvroDataToSqlServer(path.concat("\\").concat(fileInfo),table,sqlCon);
        /*
        String sql;
        StringBuilder sqlCreateTable;
        String header = "";
        String lineHeader;
        String fileName = path+"\\"+fileInfo;
        try {
            File file = new File(fileName);
            if(file.exists()) {
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), StandardCharsets.ISO_8859_1));
                int i = 0;
                while ((line = br.readLine()) != null) {
                    if (i == 0) {
                        header = "INSERT INTO " + table + "(";
                        lineHeader = line;
                        line = line.replace(";", ",").replace("\"", "");

                        sqlCreateTable = new StringBuilder("IF OBJECT_ID('" + table + "') IS NOT NULL \n" +
                                "\tDROP TABLE " + table + "\n" +
                                "\n" +
                                "SELECT ");
                        String[] value = lineHeader.split(";");
                        for (String data : value)
                            sqlCreateTable.append(" ").append(data).append(" = CAST('' AS NVARCHAR(150)),");

                        sqlCreateTable = new StringBuilder(sqlCreateTable.substring(0, sqlCreateTable.length() - 1));

                        sqlCreateTable.append(" INTO ").append(table).append(   /*" FROM " + table.replace("_DEST", "") +*/ /*";").append(" TRUNCATE TABLE ").append(table);
                        executeQuery(sqlCon, sqlCreateTable.toString());
                        header = header + line + ")";
                    } else {
                        sql = header + "VALUES (";
                        String[] value = line.split(";");
                        for (String data : value) {
                            if (data.contains("\"")) {
                                data = data.replace("'", "''");
                                sql = sql.concat(data).replace("\"", "'").concat(",");
                            } else if (data.equals("")) {
                                sql = sql.concat("NULL").concat(",");
                            }else
                                sql = sql.concat(data).concat(",");
                        }
                        sql = " IF OBJECTPROPERTY(OBJECT_ID('" + table + "'), 'TableHasIdentity') = 1 SET IDENTITY_INSERT " + table + " ON ; "
                                + sql.substring(0, sql.length() - 1) + "); "
                                + " IF OBJECTPROPERTY(OBJECT_ID('" + table + "'), 'TableHasIdentity') = 1  SET IDENTITY_INSERT " + table + " OFF ; ";
                       executeQuery(sqlCon,sql);
                    }
                    i++;
                }
                br.close();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        */
    }

    private static String escapeDoubleQuotes(String value) {
        return value.replaceAll("\"", "\"\"");
    }

    private static int writeHeaderLine(ResultSet result,FileWriter fileWriter) throws SQLException, IOException {
        // write header line containing column names
        ResultSetMetaData metaData = result.getMetaData();
        int numberOfColumns = metaData.getColumnCount();
        String headerLine = "";

        // exclude the first column which is the ID field
        for (int i = 1; i <= numberOfColumns; i++) {
            String columnName = metaData.getColumnName(i);
            headerLine = headerLine.concat(columnName).concat(";");
        }

        fileWriter.write(headerLine.substring(0, headerLine.length() - 1)+"\n");

        return numberOfColumns;
    }

    public static void updateSelectTable(String table, Connection sqlCon)
    {
        executeQuery(sqlCon,updateSelectTableQuery(table));
    }

    public static String updateSelectTableQuery(String table)
    {
        return  "DECLARE @cbModification DATETIME = (SELECT MAX(cbModification) FROM " + table + ");" +
                "DECLARE @table VARCHAR(150) = '" + table + "'" +
                "IF EXISTS(SELECT 1 FROM config.SelectTable WHERE tableName = @table) \n" +
                " UPDATE config.SelectTable " +
                "     SET lastSynchro =  @cbModification \n" +
                " WHERE tableName = @table; \n" +
                "         ELSE \n" +
                "             INSERT INTO config.SelectTable(tableName,lastSynchro) VALUES(@table,@cbModification) \n";
    }

    public static void getData(Connection sqlCon, String query,String table,String path,String file)
    {
        //updateSelectTable(table, sqlCon);

        writeToFileAvro(path + "\\" + file, query, sqlCon);
        //writeOnFile(path+"\\"+file, query, sqlCon);
    }

    public static void executeQuery(Connection sqlCon, String query)
    {
        try {
            stmt = sqlCon.createStatement();
            stmt.execute(query);
            stmt.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void disableTrigger (Connection sqlCon,String table){
        executeQuery(sqlCon,
                "                DISABLE TRIGGER ALL ON [dbo].["+table+"] ;\n");
    }

    public static void enableTrigger (Connection sqlCon,String table){
        executeQuery(sqlCon,
                "                ENABLE TRIGGER ALL ON [dbo].["+table+"] ;\n");
    }

    public static ResultSet executeQueryResult(Connection sqlCon, String query)
    {
        ResultSet resultSet = null;
        try {
            stmt = sqlCon.createStatement();
            resultSet = stmt.executeQuery(query);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return resultSet;
    }


    public static void initTableParam(Connection sqlCon,String table,String configTable,String keyColumn)
    {
        String query = " BEGIN TRY\n" +
                "DECLARE @MaColonne AS VARCHAR(250);\n" +
                "DECLARE @MonSQL AS VARCHAR(MAX)=''; \n" +
                "DECLARE @TableName AS VARCHAR(100) = '"+table+"';\n" +
                "DECLARE @configTable AS VARCHAR(100) = '"+configTable+"';\n" +
                "DECLARE @keyColumn AS VARCHAR(250) = '"+keyColumn+"';\n" +
                "\n" +
                "SELECT @MonSQL = ' INSERT INTO config.'+@configTable+' SELECT '+@keyColumn+',cbMarq,GETDATE() FROM ( SELECT DISTINCT '+@keyColumn+',cbMarq FROM '+@TableName\n" +
                "+' EXCEPT SELECT '+@keyColumn+',cbMarq FROM config.'+@configTable + ')A'" +
                "\n" +
                "EXEC (@MonSQL)\n" +
                "\n" +
                "END TRY\n" +
                "BEGIN CATCH \n" +
                "\tINSERT INTO config.DB_Errors\n" +
                "\tVALUES\n" +
                "\t(\n" +
                "\t\tSUSER_SNAME(),\n" +
                "\t\tERROR_NUMBER(),\n" +
                "\t\tERROR_STATE(),\n" +
                "\t\tERROR_SEVERITY(),\n" +
                "\t\tERROR_LINE(),\n" +
                "\t\tERROR_PROCEDURE(),\n" +
                "\t\tERROR_MESSAGE(),\n" +
                "\t\t@TableName,\n" +
                "\t\t@MonSQL,\n" +
                "\t\tGETDATE()\n" +
                "\t);\n" +
                "END CATCH\n";
        executeQuery(sqlCon, query);
    }
}
