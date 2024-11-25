
import org.apache.avro.InvalidAvroMagicException;
import org.json.simple.JSONObject;
import java.io.*;
import java.math.BigDecimal;
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
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.util.List;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class Table {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.s";
    private static String extension = ".csv";
    private static Statement stmt;

    public static String updateSelectTable(String table,boolean existsCbModification){
        return "BEGIN \n" +
                "\nDECLARE @TableName AS VARCHAR(100) = '"+table+"'; \n" +
                "\nDECLARE @cbModification VARCHAR(100) = "+ ((!existsCbModification) ? "NULL" : "ISNULL((SELECT CONVERT(NVARCHAR(100),(SELECT MAX(cbModification) FROM " + table + "),25)),'1901-01-01');\n") +

                "IF EXISTS(SELECT 1 FROM config.SelectTable WHERE tableName = @TableName) \n" +
                " UPDATE config.SelectTable " +
                "     SET lastSynchro =  CONVERT(DATETIME,@cbModification,20) \n" +
                "     , isLoaded =  CASE WHEN CONVERT(DATETIME,@cbModification,20) = lastSynchro THEN " +
                "                               CASE WHEN ISNULL(isLoaded,0) < 2 THEN  ISNULL(isLoaded,0) + 1 ELSE  ISNULL(isLoaded,0) END" +
                "                       ELSE 0 END\n" +
                " WHERE tableName = @TableName; \n" +
                "         ELSE \n" +
                "             INSERT INTO config.SelectTable(tableName,lastSynchro,isLoaded) VALUES(@TableName,CONVERT(DATETIME,@cbModification,20),0); \n"+

                "\n" +
                "END";
    }
    public static String selectSourceTable(String table,String dataSource,boolean existsCbModification,String sourceColumn){
        return "BEGIN \n" +
                "\nDECLARE @TableName AS VARCHAR(100) = '"+table+"'; \n" +
                "DECLARE @sourceColumn AS VARCHAR(100) = '"+sourceColumn+"'; \n" +
                "DECLARE @querySourceColumn AS NVARCHAR(MAX) = ''; \n" +
                "\nDECLARE @cbModification VARCHAR(100) = "+ ((!existsCbModification) ? "NULL" : "ISNULL((SELECT CONVERT(NVARCHAR(100),(SELECT MAX(cbModification) FROM " + table + "),25)),'1901-01-01');\n") +
                "\nDECLARE @lastSynchro VARCHAR(100) = ISNULL((SELECT CONVERT(NVARCHAR(100),(SELECT CASE WHEN ISNULL(isLoaded,0) = 2 THEN lastSynchro ELSE DATEADD(HOUR,-1,lastSynchro) END FROM config.SelectTable WHERE tableName = @TableName),25)),'1901-01-01');" +
                "\nDROP TABLE IF EXISTS #sourceColumn\n" +
                "SELECT\t[value]\n" +
                "INTO #sourceColumn\n" +
                "FROM STRING_SPLIT(@sourceColumn,',')\n" +
                "WHERE value <> '';\n"+
        /*"IF EXISTS(SELECT 1 FROM config.SelectTable WHERE tableName = @TableName) \n" +
                " UPDATE config.SelectTable " +
                "     SET lastSynchro =  CONVERT(DATETIME,@cbModification,20) \n" +
                "     , isLoaded =  CASE WHEN CONVERT(DATETIME,@cbModification,20) = lastSynchro THEN " +
                "                               CASE WHEN ISNULL(isLoaded,0) < 2 THEN  ISNULL(isLoaded,0) + 1 ELSE  ISNULL(isLoaded,0) END" +
                "                       ELSE 0 END\n" +
                " WHERE tableName = @TableName; \n" +
                "         ELSE \n" +
                "             INSERT INTO config.SelectTable(tableName,lastSynchro,isLoaded) VALUES(@TableName,CONVERT(DATETIME,@cbModification,20),0); \n"+*/
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
                "INNER JOIN sys.types t\n" +
                "    ON col.user_type_id = t.user_type_id\n" +
                "WHERE tab.name = @TableName\n" +
                "AND col.name NOT IN ('DataBaseSource','cbMarqSource','cbMarq')\n" +
                "AND t.name NOT IN ('varbinary')\n" +
                "AND col.name NOT IN (SELECT CONCAT([value],'Source') FROM #sourceColumn)\n" +
                "\n" +
                "OPEN @getid\n" +
                "FETCH NEXT\n" +
                "FROM @getid INTO @MaColonne\n" +
                "WHILE @@FETCH_STATUS = 0\n" +
                "BEGIN\n" +
                " SELECT @MonSQL = @MonSQL+ ',' + @MaColonne \n" +
                "\n" +
                " FETCH NEXT\n" +
                "    FROM @getid INTO @MaColonne /*, @name*/\n" +
                "END\n" +
                "CLOSE @getid\n" +
                "DEALLOCATE @getid\n" +
                "SELECT @MonSQL = SUBSTRING(@MonSQL,2,LEN(@MonSQL)) \n" +
                "\n" +
                "\nSELECT @querySourceColumn = STRING_AGG(CAST(CONCAT([value],'Source = ',[value]) AS VARCHAR(MAX)),',')\n" +
                "FROM #sourceColumn\n"+
                "SELECT @MonSQL = 'DECLARE @databaseSource AS VARCHAR (150) = '''+@databaseSource+'''; SELECT ' + @MonSQL \n" +
                "+',cbMarqSource = [cbMarq],[DataBaseSource] = @databaseSource '+(CASE WHEN ISNULL(@querySourceColumn,'')<> '' THEN ','+ @querySourceColumn ELSE '' END)+' FROM '\n" +
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

    public static String selectSourceTable(String table, String dataSource, JSONObject type,String sourceColumn){
        return "BEGIN \n" +
                "DECLARE @TableName AS VARCHAR(100) = '"+table+"'; \n" +
                "DECLARE @sourceColumn AS VARCHAR(100) = '"+sourceColumn+"'; \n"+
                "DECLARE @querySourceColumn AS NVARCHAR(MAX) = '' \n"+
                "DECLARE @cbModification VARCHAR(100) = ISNULL((SELECT CONVERT(NVARCHAR(100),(SELECT MAX(cbModification) FROM " + table + "),25)),'1901-01-01');" +
                "DECLARE @lastSynchro VARCHAR(100) = ISNULL((SELECT CONVERT(NVARCHAR(100),(SELECT CASE WHEN ISNULL(isLoaded,0) = 2 THEN lastSynchro ELSE DATEADD(HOUR,-1,lastSynchro) END FROM config.SelectTable WHERE tableName = @TableName),25)),'1901-01-01');" +
                "DROP TABLE IF EXISTS #sourceColumn\n" +
                "SELECT\t[value]\n" +
                "INTO #sourceColumn\n" +
                "FROM STRING_SPLIT(@sourceColumn,',')\n" +
                "WHERE value <> '';\n"+
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
                "AND col.name NOT IN (SELECT CONCAT([value],'Source') FROM #sourceColumn)\n" +
                "\n" +
                "OPEN @getid\n" +
                "FETCH NEXT\n" +
                "FROM @getid INTO @MaColonne\n" +
                "WHILE @@FETCH_STATUS = 0\n" +
                "BEGIN\n" +
                " SELECT @MonSQL = @MonSQL+ ',' + @MaColonne \n" +
                "\n" +
                " FETCH NEXT\n" +
                "    FROM @getid INTO @MaColonne /*, @name*/\n" +
                "END\n" +
                "CLOSE @getid\n" +
                "DEALLOCATE @getid\n" +
                "SELECT @MonSQL = SUBSTRING(@MonSQL,2,LEN(@MonSQL)) \n" +
                "\n" +
                "SELECT @querySourceColumn = STRING_AGG(CAST(CONCAT([value],'Source = ',[value]) AS VARCHAR(MAX)),',')\n" +
                "FROM #sourceColumn\n"+
                "SELECT @MonSQL = 'SELECT ' + @MonSQL + ',[cbProt],[cbCreateur],[cbModification],[cbReplication],[cbFlag],cbMarqSource = [cbMarq],[DataBaseSource] = ''"+dataSource+"'''+(CASE WHEN ISNULL(@querySourceColumn,'')<> '' THEN ','+ @querySourceColumn ELSE '' END)+'  FROM '+ @TableName +'" +
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
                "    FROM @getid INTO @MaColonne /*, @name*/\n" +
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
                "    FROM @getid INTO @MaColonne /*, @name*/\n" +
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
                "    FROM @getid INTO @MaColonne /*, @name*/\n" +
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
                "    FROM @getid INTO @MaColonne /*, @name*/\n" +
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
        //AvroConverter.writeToFileAvro(path + "\\" + file, query, sqlCon);
        CsvConverter.writeOnFile(path + "\\" + file, query, sqlCon);
        query = listDeleteItem(table,listTable);
        executeQuery(sqlCon, query);
    }

    public static void listExistsKeys(Connection sqlCon, String path, String file,String table,String keySource,String limitDate)
    {
        String query = listExistsKey(table,keySource,limitDate);
        AvroConverter.writeToFileAvro(path + "\\" + file, query, sqlCon);
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


    public static void loadDeleteFile(String path,Connection sqlCon,String file,String tableName,String keySource,String listKeys) {
        File dir = new File(path);
        FilenameFilter filter = (dir1, name) -> name.startsWith("deleteList"+file);
        String [] children = dir.list(filter);
        if (children == null) {
            System.out.println("Either dir does not exist or is not a directory");
        } else {
            for (String filename : children) {
                readOnFile(path, filename, tableName + "_SUPPR", sqlCon);
                deleteItem(sqlCon, tableName,filename,keySource,listKeys);
            }
        }
    }

    public static String [] getFile(String path,String file){
        File dir = new File(path);
        FilenameFilter filter = (dir1, name) -> name.startsWith(file) ;
        return dir.list(filter);
    }

    /***
    IF OBJECT_ID('{tablename}_SUPPR') IS NOT NULL
	DELETE src
	FROM {tablename} src
	WHERE EXISTS (
			SELECT 1
			FROM {tablename}_SUPPR suppr
			WHERE 1 = 1
				AND ISNULL(src.{keys}, '') = ISNULL(suppr.{keys}, '')
				AND ISNULL(src.{keysSource}Source, '') = ISNULL(suppr.{keys}, '')
			)

     ***/
    public static void deleteItem(Connection sqlCon, String tablename,String fileName,String keySource,String listKeys)
    {
        StringBuilder sql = new StringBuilder("\n" +
                "BEGIN TRY\n" +
                "DECLARE @MaColonne AS VARCHAR(250);\n" +
                "DECLARE @MonSQL AS NVARCHAR(MAX)=''; \n" +
                "DECLARE @TableName AS VARCHAR(100) = '"+tablename+"';\n" +
                "DECLARE @filename AS VARCHAR(100) = '"+fileName+"';\n" +
                "DECLARE @keySource AS VARCHAR(100) = '"+keySource+"';\n" +
                "DECLARE @listKeys AS VARCHAR(100) = '"+listKeys+"';\n" +
                "DECLARE @listKeysQuery AS VARCHAR(MAX) = '';\n" +
                "\n" +
                "SELECT @listKeysQuery = STRING_AGG (CAST('ISNULL(src.'+[value] +','''') = ISNULL(suppr.'+[value]+','''')'  AS VARCHAR(MAX)), ' AND ')\n" +
                "FROM STRING_SPLIT(@listKeys,',')\n" +
                "\n" +
                "SELECT @MonSQL = 'IF OBJECT_ID('''+@TableName+'_SUPPR'') IS NOT NULL '\n" +
                "\t\t\t\t +'\tDELETE src \n" +
                "\t\t\t\t\tFROM '+@TableName+' src \n" +
                "\t\t\t\t\tWHERE EXISTS (\tSELECT 1 \n" +
                "\t\t\t\t\t\t\t\t\tFROM '+@TableName+'_SUPPR suppr\n" +
                "\t\t\t\t\t\t\t\t\tWHERE 1=1'\n" +
                "\t\t\t\t\t\t\t\t\t+ CASE WHEN ISNULL(@listKeysQuery,'') <> '' THEN ' AND '+ @listKeysQuery ELSE '' END \n" +
                "\t\t\t\t\t\t\t\t\t+ CASE WHEN ISNULL(@keySource,'') <> '' THEN ' AND '+ ('ISNULL(src.'+ @keySource +'Source,'''') = ISNULL(suppr.' + @keySource+' ,'''')') ELSE '' END\n" +
                "\t\t\t\t\t\t\t\t\t+')'\n" +
                "\n" +
                "\n" +
                "exec sp_executesql @MonSQL\n" +
                "\n" +
                "END TRY\n" +
                "BEGIN CATCH \n" +
                "INSERT INTO config.DB_Errors\n" +
                "VALUES\n" +
                "(\n" +
                "SUSER_SNAME(),\n" +
                "ERROR_NUMBER(),\n" +
                "ERROR_STATE(),\n" +
                "ERROR_SEVERITY(),\n" +
                "ERROR_LINE(),\n" +
                "ERROR_PROCEDURE(),\n" +
                "ERROR_MESSAGE(),\n" +
                "'Suppr del ' + @fileName,\n" +
                "@MonSQL,\n" +
                "GETDATE()\n" +
                ");\n" +
                "END CATCH;");
            executeQuery(sqlCon, sql.toString());
    }

    public static String listExistsKey(String table,String keySource,String limitDate){
        return "BEGIN TRY\n" +
                "    DECLARE @MaColonne AS VARCHAR(250);\n" +
                "    DECLARE @MonSQL AS VARCHAR(MAX)=''; \n" +
                "    DECLARE @TableName AS VARCHAR(100) = '" + table +"';\n" +
                "    DECLARE @keyColumns AS VARCHAR(100) = ' "+ keySource+"'; \n" +
                "     DECLARE @limitDate AS INT = "+limitDate+"; \n" +
                "    \n" +
                "\t\n" +
                "\tSELECT @MonSQL = CONCAT('SELECT '+@keyColumns,',DataBaseSource = DB_Name()  FROM ', @TableName,CASE WHEN @limitDate = 1 THEN ' WHERE cbModification > DATEADD(DAY,-365,GETDATE()) ' ELSE '' END);\n" +
                "    \n" +
                "\tEXEC (@MonSQL)\n" +
                "                \n" +
                "    END TRY\n" +
                "    BEGIN CATCH \n" +
                "    INSERT INTO config.DB_Errors\n" +
                "    VALUES\n" +
                "    (\n" +
                "    SUSER_SNAME(),\n" +
                "    ERROR_NUMBER(),\n" +
                "    ERROR_STATE(),\n" +
                "    ERROR_SEVERITY(),\n" +
                "    ERROR_LINE(),\n" +
                "    ERROR_PROCEDURE(),\n" +
                "    ERROR_MESSAGE(),\n" +
                "    @TableName,\n" +
                "    @MonSQL,\n" +
                "    GETDATE()\n" +
                "    );\n" +
                "    END CATCH";
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
        StringBuilder sql = new StringBuilder("\n" +
        "BEGIN TRY\n" +
                "\n" +
                "DECLARE @tableName VARCHAR(150) = '" + tableName + "'\n" +
                "DECLARE @tableNameDest VARCHAR(150) = '" + tableNameDest + "'\n" +
                "DECLARE @keyJoin VARCHAR(150) = '"+ key +"'\n" +
                "DECLARE @exclusionColumn VARCHAR(MAX) = '"+ exclude +"'\n" +
                "DECLARE @filename VARCHAR(150) = '"+ filename +"'\n" +
                "DECLARE @columnsSource NVARCHAR(MAX);\n" +
                "DECLARE @columnsDest NVARCHAR(MAX);\n" +
                "DECLARE @sql NVARCHAR(MAX);\n" +
                "DECLARE @columns NVARCHAR(MAX);\n" +
                "DECLARE @keyValue VARCHAR(MAX) = ''\n" +
                "DROP TABLE IF EXISTS #keyJoin;\n" +
                "DROP TABLE IF EXISTS #exclusionColumn;\n" +
                "/* Obtenir la liste des colonnes non-IDENTITY*/\n" +
                "SELECT *\n" +
                "\tINTO #keyJoin\n" +
                "FROM STRING_SPLIT(@keyJoin,',')\n" +
                "\n" +
                "SELECT *\n" +
                "\tINTO #exclusionColumn\n" +
                "FROM STRING_SPLIT(@exclusionColumn,',')\n" +
                "\n" +
                "SELECT @columnsSource = STRING_AGG(CAST(col.name AS VARCHAR(MAX)),',')\n" +
                "FROM sys.tables tab\n" +
                "INNER JOIN sys.columns col\n" +
                "\tON tab.object_id = col.object_id\n" +
                "INNER JOIN sys.types t\n" +
                "\tON col.user_type_id = t.user_type_id\n" +
                "WHERE tab.name = @tableName\n" +
                "AND t.name NOT IN ('varbinary')\n" +
                "AND col.is_identity <> 1\n" +
                "\n" +
                ";\n" +
                "WITH _Source_ AS (\n" +
                "SELECT  \n" +
                "    CASE \n" +
                "        /* Vérifier si la colonne existe dans F_COMPTETG_DEST*/\n" +
                "        WHEN EXISTS (\n" +
                "            SELECT 1 \n" +
                "            FROM sys.columns c\n" +
                "            INNER JOIN sys.tables t ON t.object_id = c.object_id\n" +
                "            WHERE t.name = @tableNameDest\n" +
                "            AND c.name = col.name\n" +
                "        ) AND col.name NOT IN (SELECT value FROM #keyJoin) AND col.name NOT IN (SELECT value FROM #exclusionColumn)\n" +
                "        THEN ''+col.name +' = dest.' + col.name   /* Si la colonne existe, on l'inclut*/\n" +
                "    END AS Col\n" +
                "FROM sys.tables tab\n" +
                "INNER JOIN sys.columns col\n" +
                "    ON tab.object_id = col.object_id\n" +
                "INNER JOIN sys.types t\n" +
                "    ON col.user_type_id = t.user_type_id\n" +
                "WHERE tab.name = @tableName\n" +
                "AND t.name NOT IN ('varbinary')\n" +
                "AND col.is_identity <> 1\n" +
                ")\n" +
                "SELECT @columnsDest = STRING_AGG(CAST(col AS VARCHAR(MAX)),',')\n" +
                "    FROM _Source_;\n" +
                "\t\n" +
                "SELECT @keyValue = STRING_AGG(  CAST('src.'+col.name + ' = dest.' + col.name  AS VARCHAR(MAX)), ' AND ')\n" +
                "FROM sys.tables tab\n" +
                "INNER JOIN sys.columns col\n" +
                "    ON tab.object_id = col.object_id\n" +
                "INNER JOIN sys.types t\n" +
                "    ON col.user_type_id = t.user_type_id\n" +
                "WHERE tab.name = @tableName\n" +
                "AND col.name IN (SELECT [value] FROM #keyJoin)\n" +
                "\n" +
                "SELECT @sql =\n" +
                "'SET DATEFORMAT ymd; '+\n" +
                "\n" +
                "' IF OBJECT_ID('''+@tableNameDest+''') IS NOT NULL ' + \n" +
                "' UPDATE src SET ' + @columnsDest +\n" +
                "' FROM '+ @tableNameDest + ' dest ' +\n" +
                "' INNER JOIN ' + @tableName + ' src ON ' + @keyValue \n" +
                "\n" +
                "exec sp_executesql @sql\n" +
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
                "\t\t@TableName + ' ' + @filename,\n" +
                "\t\t@sql,\n" +
                "\t\tGETDATE()\n" +
                "\t);\n" +
                "END CATCH");
        return sql.toString();
    }

    /**
     * Compare les données entre tableName et tableNameDest
     * @param tableName
     * @param tableNameDest
     * @param keyJoin
     * @param fileName
     * @param increment
     * @param isSource
     * @param incrementValue
     * @param keySource
     * @param setToNull
     * @return
     */
    public static String insertTmpTable (String tableName,String tableNameDest,String keyJoin,String fileName,int increment,int isSource,String incrementValue,String keySource,String setToNull){
        StringBuilder sql = new StringBuilder("\n" +
                "\n" +
                "DECLARE @tableName VARCHAR(150) = '"+ tableName +"'\n" +
                "DECLARE @tableNameDest VARCHAR(150) = '"+ tableNameDest +"'\n" +
                "DECLARE @keyJoin VARCHAR(150) = '"+ keyJoin +"'\n" +
                "DECLARE @filename VARCHAR(150) = '"+ fileName + "'\n" +
                "DECLARE @increment INT = "+ increment +"\n" +
                "DECLARE @isSource INT  = "+ isSource +"\n" +
                "DECLARE @incrementValue VARCHAR(50) = '"+ incrementValue +"'\n" +
                "DECLARE @keySource VARCHAR(50) = '"+ keySource +"'\n" +
                "DECLARE @setToNull VARCHAR(50) = '"+ setToNull +"'\n" +
                "DECLARE @keyValue VARCHAR(MAX) = ''\n" +
                "DECLARE @columnsSource NVARCHAR(MAX);\n" +
                "DECLARE @columnsDest NVARCHAR(MAX);\n" +
                "DECLARE @sql NVARCHAR(MAX);\n" +
                "DECLARE @columns NVARCHAR(MAX);\n" +
                "DROP TABLE IF EXISTS #keyJoin;\n" +
                "DROP TABLE IF EXISTS #setToNull;\n" +
                "/* Obtenir la liste des colonnes non-IDENTITY*/\n" +
                "SELECT *\n" +
                "\tINTO #keyJoin\n" +
                "FROM STRING_SPLIT(@keyJoin,',')\n" +
                "SELECT *\n" +
                "\tINTO #setToNull\n" +
                "FROM STRING_SPLIT(@setToNull,',')\n" +
                "\n" +
                "    SELECT @columnsSource = STRING_AGG(CAST(col.name  AS VARCHAR(MAX)),',')\n" +
                "    FROM sys.tables tab\n" +
                "\tINNER JOIN sys.columns col\n" +
                "\t\tON tab.object_id = col.object_id\n" +
                "\tINNER JOIN sys.types t\n" +
                "\t\tON col.user_type_id = t.user_type_id\n" +
                "\tWHERE tab.name = @tableName\n" +
                "\tAND t.name NOT IN ('varbinary')\n" +
                "\tAND col.is_identity <> 1\n" +
                "\n" +
                ";\n" +
                "WITH _Source_ AS (\n" +
                "SELECT  \n" +
                "    CASE \n" +
                "        /* Vérifier si la colonne existe dans F_COMPTETG_DEST */\n" +
                "        WHEN EXISTS (\n" +
                "            SELECT 1 \n" +
                "            FROM sys.columns c\n" +
                "            INNER JOIN sys.tables t ON t.object_id = c.object_id\n" +
                "            WHERE t.name = @tableNameDest\n" +
                "            AND c.name = col.name\n" +
                "        ) \n" +
                "        THEN CASE WHEN @increment  = 1 AND col.name = @incrementValue THEN '(SELECT ISNULL((SELECT MAX('+@incrementValue+') FROM '+@tableName+'),0)) + ROW_NUMBER() OVER(ORDER BY dest.' + @incrementValue + ' ) AS ' + col.name\n" +
                "\t\t\t\t\tWHEN @isSource = 1 AND col.name = @keySource+'Source' THEN  'dest.'+@keySource +' AS ' + col.name\n" +
                "\t\t\t\t\tWHEN col.name IN (SELECT [value] FROM #setToNull) THEN  'NULL AS ' + col.name \n" +
                "\t\t\t\t\tELSE  'dest.' + col.name  END /* Si la colonne existe, on l'inclut */\n" +
                "        ELSE 'NULL AS ' + col.name  /* Si elle n'existe pas, on met NULL */\n" +
                "    END AS Col\n" +
                "FROM sys.tables tab\n" +
                "INNER JOIN sys.columns col\n" +
                "    ON tab.object_id = col.object_id\n" +
                "INNER JOIN sys.types t\n" +
                "    ON col.user_type_id = t.user_type_id\n" +
                "WHERE tab.name = @tableName\n" +
                "AND t.name NOT IN ('varbinary')\n" +
                "AND col.is_identity <> 1\n" +
                ")\n" +
                "SELECT @columnsDest = STRING_AGG(CAST(col AS VARCHAR(MAX)),',')\n" +
                "    FROM _Source_;\n" +
                "\t\n" +
                "\t\n" +
                "SELECT @keyValue = STRING_AGG( CAST(CASE WHEN @isSource = 1 AND col.name = @keySource THEN 'ISNULL(dest.'+ @keySource + ','''') = ISNULL(src.' + @keySource + 'Source,'''')' ELSE 'ISNULL(src.'+col.name + ','''') = ISNULL(dest.' + col.name +','''')' END AS VARCHAR(MAX)), ' AND ')\n" +
                "FROM sys.tables tab\n" +
                "INNER JOIN sys.columns col\n" +
                "    ON tab.object_id = col.object_id\n" +
                "INNER JOIN sys.types t\n" +
                "    ON col.user_type_id = t.user_type_id\n" +
                "WHERE tab.name = @tableName\n" +
                "AND col.name IN (SELECT [value] FROM #keyJoin)\n" +
                "\n" +
                "\n" +
                "SELECT @sql =\n" +
                "'SET DATEFORMAT ymd; '+\n" +
                "\n" +
                "\n" +
                "' DROP TABLE IF EXISTS ' + @tableName + '_TMP\n" +
                "' + ' SELECT ' + @columnsDest + ' INTO ' + @tableName + '_TMP' \n" +
                "+ ' FROM ' + @tableNameDest + ' dest '\n" +
                "+ ' LEFT JOIN ' + @tableName + ' src '\n" +
                "+ ' ON ' + @keyValue\n" +
                "+ ' WHERE src.' + (SELECT TOP 1 value FROM #keyJoin) + ' IS NULL;'\n" +
                "\n" +
                "exec sp_executesql @sql");
        return sql.toString();
    }

    /**
     *
     * @param tableName Nom de la table source
     * @param tableNameDest Nom de la table temporaire crée lors de l'import
     * @param keyJoin Clé de jointure entre la table source et temporaire
     * @param fileName nom du fichier traité
     * @param increment détermine si une clé doit être incrémenté
     * @param isSource
     *    LEFT JOIN F_DEPOTEMPL src ON ISNULL(dest.keySource, '') = ISNULL(src.{keySource}Source, '')
            AND ISNULL(src.DataBaseSource, '') = ISNULL(dest.DataBaseSource, '')
     * @param incrementValue colonne à incrémenter
     * @param keySource
     * @param setToNull colonne à mettre a null
     * @return
     */
    public static String insertTable (String tableName,String tableNameDest,String keyJoin,String fileName,int increment,int isSource,String incrementValue,String keySource,String setToNull){
        StringBuilder sql = new StringBuilder("\n" +
                "\n" +
                "DECLARE @tableName VARCHAR(150) = '"+ tableName +"'\n" +
                "DECLARE @tableNameDest VARCHAR(150) = '"+ tableNameDest +"'\n" +
                "DECLARE @keyJoin VARCHAR(150) = '"+ keyJoin +"'\n" +
                "DECLARE @filename VARCHAR(150) = '"+ fileName + "'\n" +
                "DECLARE @increment INT = "+ increment +"\n" +
                "DECLARE @isSource INT  = "+ isSource +"\n" +
                "DECLARE @incrementValue VARCHAR(50) = '"+ incrementValue +"'\n" +
                "DECLARE @keySource VARCHAR(50) = '"+ keySource +"'\n" +
                "DECLARE @setToNull VARCHAR(50) = '"+ setToNull +"'\n" +
                "DECLARE @keyValue VARCHAR(MAX) = ''\n" +
                "DECLARE @columnsSource NVARCHAR(MAX);\n" +
                "DECLARE @columnsDest NVARCHAR(MAX);\n" +
                "DECLARE @sql NVARCHAR(MAX);\n" +
                "DECLARE @columns NVARCHAR(MAX);\n" +
                "DROP TABLE IF EXISTS #keyJoin;\n" +
                "DROP TABLE IF EXISTS #setToNull;\n" +
                "/* Obtenir la liste des colonnes non-IDENTITY */\n" +
                "SELECT *\n" +
                "\tINTO #keyJoin\n" +
                "FROM STRING_SPLIT(@keyJoin,',')\n" +
                "SELECT *\n" +
                "\tINTO #setToNull\n" +
                "FROM STRING_SPLIT(@setToNull,',')\n" +
                "\n" +
                "    SELECT @columnsSource = STRING_AGG(CAST(col.name AS VARCHAR(MAX)),',')\n" +
                "    FROM sys.tables tab\n" +
                "\tINNER JOIN sys.columns col\n" +
                "\t\tON tab.object_id = col.object_id\n" +
                "\tINNER JOIN sys.types t\n" +
                "\t\tON col.user_type_id = t.user_type_id\n" +
                "\tWHERE tab.name = @tableName\n" +
                "\tAND t.name NOT IN ('varbinary')\n" +
                "\tAND col.is_identity <> 1\n" +
                "\tAND (col.name NOT LIKE 'cb%' OR col.name = 'cbMarqSource')\n" +
                "\n" +
                ";\n" +
                "WITH _Source_ AS (\n" +
                "SELECT  \n" +
                "    CASE \n" +
                "        /* Vérifier si la colonne existe dans F_COMPTETG_DEST */\n" +
                "        WHEN EXISTS (\n" +
                "            SELECT 1 \n" +
                "            FROM sys.columns c\n" +
                "            INNER JOIN sys.tables t ON t.object_id = c.object_id\n" +
                "            WHERE t.name = @tableNameDest\n" +
                "            AND c.name = col.name\n" +
                "        ) \n" +
                "        THEN CASE WHEN @increment  = 1 AND col.name = @incrementValue THEN '(SELECT ISNULL((SELECT MAX('+@incrementValue+') FROM '+@tableName+'),0)) + ROW_NUMBER() OVER(ORDER BY dest.' + @incrementValue + ' ) AS ' + col.name\n" +
                "\t\t\t\t\tWHEN @isSource = 1 AND col.name = @keySource+'Source' THEN  'dest.'+@keySource +' AS ' + col.name\n" +
                "\t\t\t\t\tWHEN col.name IN (SELECT [value] FROM #setToNull) THEN  'NULL AS ' + col.name \n" +
                "\t\t\t\t\tELSE  'dest.' + col.name  END /* Si la colonne existe, on l'inclut */\n" +
                "        ELSE 'NULL AS ' + col.name  /* Si elle n'existe pas, on met NULL */\n" +
                "    END AS Col \n" +
                " FROM sys.tables tab\n" +
                "INNER JOIN sys.columns col\n" +
                "    ON tab.object_id = col.object_id\n" +
                "INNER JOIN sys.types t\n" +
                "    ON col.user_type_id = t.user_type_id\n" +
                "WHERE tab.name = @tableName\n" +
                "AND t.name NOT IN ('varbinary')\n" +
                "AND col.is_identity <> 1\n" +
                "AND (col.name NOT LIKE 'cb%' OR col.name = 'cbMarqSource')\n" +
                ")\n" +
                "SELECT @columnsDest = STRING_AGG(CAST(col AS VARCHAR(MAX)),',')\n" +
                "    FROM _Source_;\n" +
                "\t\n" +
                "\t\n" +
                "SELECT @keyValue = STRING_AGG( CAST(CASE WHEN @isSource = 1 AND col.name = @keySource THEN 'dest.'+ @keySource + ' = src.' + @keySource + 'Source' ELSE 'src.'+col.name + ' = dest.' + col.name END AS VARCHAR(MAX)), ' AND ')\n" +
                "FROM sys.tables tab\n" +
                "INNER JOIN sys.columns col\n" +
                "    ON tab.object_id = col.object_id\n" +
                "INNER JOIN sys.types t\n" +
                "    ON col.user_type_id = t.user_type_id\n" +
                "WHERE tab.name = @tableName\n" +
                "AND col.name IN (SELECT [value] FROM #keyJoin)\n" +
                "\n" +
                "SELECT @sql =\n" +
                "'SET DATEFORMAT ymd; '+\n" +
                "'IF OBJECT_ID('''+ @tableNameDest+''') IS NOT NULL '+\n" +
                "'INSERT INTO ' + @tableName + ' (' + @columnsSource + ')'\n" +
                "+ ' SELECT ' + @columnsDest\n" +
                "+ ' FROM ' + @tableNameDest + ' dest '\n" +
                "+ ' LEFT JOIN ' + @tableName + ' src '\n" +
                "+ ' ON ' + @keyValue\n" +
                "+ ' WHERE src.' + (SELECT TOP 1 value FROM #keyJoin) + ' IS NULL;'\n" +
                ";\n" +
                "\n" +
                "BEGIN TRY \n" +
                "\texec sp_executesql @sql\n" +
                "\n" +
                "END TRY \n" +
                "BEGIN CATCH \n" +
                "\tINSERT INTO config.DB_Errors VALUES \n" +
                " (SUSER_SNAME(),\n" +
                " ERROR_NUMBER(),\n" +
                " ERROR_STATE(),\n" +
                " ERROR_SEVERITY(),\n" +
                " ERROR_LINE(), \n" +
                " ERROR_PROCEDURE(), \n" +
                " ERROR_MESSAGE(), \n" +
                " 'Insert '+ @filename,\n" +
                " @sql,\n" +
                " GETDATE()); \n" +
                "END CATCH");
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

    public static void backupFile(String path,String file)
    {

        File filePath = new File(path + "\\" + file);
        if (filePath.exists())
        {
            archiveDocument(path + "\\archive", path, file);
        }
    }







    public static void readOnFile(String path,String fileInfo,String table,Connection sqlCon)
    {

        //AvroConverter.insertAvroDataToSqlServer(path.concat("\\").concat(fileInfo),table,sqlCon);
        AvroConverter.insertAvroDataToSqlServer(path.concat("\\").concat(fileInfo),table,sqlCon);
        backupFile(path, fileInfo);

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
        CsvConverter.writeOnFile(path + "\\" + file, query, sqlCon);
     //   AvroConverter.writeToFileAvro(path + "\\" + file, query, sqlCon);
    }

    public static void getDataNew(ConfigInfo configInfo, String query, String path, String file)
    {
        //writeToFileAvro(path + "\\" + file, query, sqlCon);
        //BCPExporter.exportDataToBCP(query, path + "\\" + file, configInfo.serverName, configInfo.databaseName, configInfo.userName, configInfo.password);
        BCPExporter.exportDataToBCPWithFormat(query, path + "\\" + file, path + "\\" + file.replace(".bcp",".fmt")
                , configInfo.serverName, configInfo.databaseName, configInfo.userName, configInfo.password);
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
