import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Table {

    private static BufferedWriter fileWriter;
    private static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.s";
    private static Statement stmt;
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
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
                "+ ' WHERE cbModification >= ISNULL((SELECT LastSynchro FROM config.SelectTable WHERE tableName='''+ @TableName +'''),''1900-01-01'')' \n" +
                "\n" +
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
        String query = "IF OBJECT_ID('"+table+"_DEST') IS NOT NULL \n" +
                "\tDROP TABLE "+table+"_DEST;";
        executeQuery(sqlCon, query);
    }

    public static void deleteRows(Connection sqlCon, String path,String file,String table,String isDataSource)
    {
        String query =
                " BEGIN TRY\n" +
                        "DECLARE @MonSQL AS VARCHAR(MAX)=''; \n" +
                        "DECLARE @TableName AS VARCHAR(100) = '"+table+"';\n" +
                        "DECLARE @isDataSource AS INT = "+isDataSource+"; \n" +
                        "\n" +
                        "SELECT @MonSQL = 'DELETE FROM '+@TableName+\t' WHERE EXISTS (SELECT 1 FROM '+@TableName+'_SUPPR WHERE '\n" +
                        "+@TableName+'.cbMarqSource '\n" +
                        "\n" +
                        "IF @isDataSource = 1\n" +
                        "\tSELECT @MonSQL = @MonSQL + ' AND '+@TableName+'.DataBaseSource = '+@TableName+'_SUPPR.DataBaseSource'\n" +
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
        if ((new File(path + "\\deleteList" + file)).exists())
        {
            executeQuery(sqlCon, query);
            archiveDocument(path + "\\archive", path, "deleteList" + file);
        }
    }


    public static void deleteItem(Connection sqlCon, String path,String file,String table)
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
    public static String updateTableDest(String key,String exclude,String tableName,String tableNameDest){
        String[] keys = key.split(",");
        String sql =  "\n" +
                "BEGIN TRY\n" +
                "DECLARE @MaColonne AS VARCHAR(250);\n" +
                "DECLARE @MonSQL AS VARCHAR(MAX) = ''; \n" +
                "DECLARE @isKey AS INT=CASE WHEN '"+ key +"' = '' THEN 0 ELSE 1 END; \n" +
                "DECLARE @Key AS NVARCHAR(250) = '"+key+"'; \n" +
                "DECLARE @TableName AS VARCHAR(100) = '"+tableName+"'; \n" +
                "DECLARE @TableNameDest AS VARCHAR(100) = '"+tableNameDest+"'; \n" +
                "DECLARE @getid CURSOR\n" +
                "\n" +
                "SET @getid = CURSOR FOR\n" +
                "SELECT col.name\n" +
                "FROM sys.tables tab\n" +
                "INNER JOIN sys.columns col\n" +
                "\tON tab.object_id = col.object_id\n" +
                "WHERE tab.name = @TableName\n" +
                "AND col.name NOT LIKE 'cb%'\n" +
                "AND col.name NOT IN ("+ exclude +")\n" +
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
                " SELECT @MonSQL = 'UPDATE '+@TableName+' SET '+ SUBSTRING(@MonSQL,2,LEN(@MonSQL));\n" +
                " SELECT @MonSQL = @MonSQL + ' FROM ' + @TableNameDest  +' WHERE '\n" +
                "IF @isKey = 0 \n" +
                "\tSELECT @MonSQL = @MonSQL + @TableName + '.cbMarqSource = '+ @TableNameDest+'.cbMarqSource '\n" +
                "\t\t\t\t\t\t+' AND '+@TableName + '.DataBaseSource = '+ @TableNameDest+'.DataBaseSource '\n" +
                "ELSE \n" ;
        for (int i = 0;i< keys.length;i++) {
            if(i== 0 ) {
                sql = sql + " SELECT @MonSQL = @MonSQL ";
                sql = sql + "+ @TableName + '." + keys[i] + " = '+ @TableNameDest+'." + keys[i] + " '";
            }
            if(i>0)
                sql = sql + "+' AND '+@TableName + '."+keys[i]+" = '+ @TableNameDest+'."+keys[i]+" '";
        }
        sql =  sql +
                "\n" +
                "EXEC (@MonSQL) \n" +
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
                "END CATCH";
        return sql;
    }
    public static void archiveDocument(String archive, String source,String file)
    {
        File filePath = new File(archive);
        if(!filePath.exists())
        {
            filePath.mkdir();
        }
        try {
            Files.move(Paths.get(source+"\\"+file),Paths.get(archive + "\\" + file));
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

    public static void writeOnFile(String fileName, String query, Connection sqlCon)
    {
        ResultSet result = null;
        FileWriter fileWriter = null;
        try {
            result = executeQueryResult(sqlCon,query);
            fileWriter = new  FileWriter (fileName, StandardCharsets.ISO_8859_1);
            int columnCount = writeHeaderLine(result,fileWriter);
            while (result.next()) {
                String line = "";

                for (int i = 1; i <= columnCount; i++) {
                    Object valueObject = result.getObject(i);
                    String valueString = "";

                    if (valueObject != null) valueString = valueObject.toString();

                    if (valueObject instanceof String) {
                        valueString = "\"" + escapeDoubleQuotes(valueString) + "\"";
                    }

                    if(isDateValid(valueString)){
                        valueString = "\"" + valueString.replace(".0","")+"\"";
                    }

                    line = line.concat(valueString);

                    if (i != columnCount) {
                        line = line.concat(";");
                    }
                }

                fileWriter.write(line+"\n");
                fileWriter.flush();
            }
            fileWriter.close();
            stmt.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readOnFile(String path,String fileInfo,String table,Connection sqlCon)
    {
        String line = "";
        String sql = "";
        String sqlCreateTable = "";
        String header = "";
        String lineHeader = "";
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

                        sqlCreateTable = "IF OBJECT_ID('" + table + "') IS NOT NULL \n" +
                                "\tDROP TABLE " + table + "\n" +
                                "\n" +
                                "SELECT ";
                        String[] value = lineHeader.split(";");
                        for (String data : value) {
                            sqlCreateTable = sqlCreateTable +" "+ data + " = CAST('' AS NVARCHAR(150)),";
                        }
                        sqlCreateTable = sqlCreateTable.substring(0, sqlCreateTable.length() - 1);

                        sqlCreateTable = sqlCreateTable +   " INTO " + table
                                                        +   /*" FROM " + table.replace("_DEST", "") +*/ ";" +
                                                            " TRUNCATE TABLE " + table;
                        executeQuery(sqlCon,sqlCreateTable);
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
        String query = "IF EXISTS(SELECT 1 FROM config.SelectTable WHERE tableName = '"+table+"') \n" +
                " UPDATE config.SelectTable " +
                "     SET lastSynchro = (SELECT MAX(cbModification) FROM " + table + ") \n" +
                " WHERE tableName = '" + table + "'; \n" +
                "         ELSE \n" +
                "             INSERT INTO config.SelectTable(tableName,lastSynchro) VALUES('"+ table + "',(SELECT MAX(cbModification) FROM " + table + ")) \n";
            executeQuery(sqlCon,query);
    }

    public static void getData(Connection sqlCon, String query,String table,String path,String file)
    {
        writeOnFile(path+"\\"+file, query, sqlCon);
        updateSelectTable(table, sqlCon);
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
                "SELECT @MonSQL = 'IF NOT EXISTS (SELECT 1 FROM config.SelectTable WHERE tableName='''+@TableName+''')'\n" +
                "+' INSERT INTO config.'+@configTable+' SELECT '+@keyColumn+',cbMarq FROM '+@TableName+' ELSE BEGIN '+\n" +
                "' INSERT INTO config.'+@configTable+' SELECT '+@keyColumn+',cbMarq FROM '+@TableName+' WHERE cbMarq '+\n" +
                "' > (SELECT max(cbMarq) FROM config.'+@configTable+') END'\n" +
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
