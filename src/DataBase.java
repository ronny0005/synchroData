import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class DataBase {

    public static JSONArray getInfoConnexion(String fileName)
    {
        try {
            JSONParser parser = new JSONParser();
            FileReader file = new FileReader(fileName);
            JSONArray obj = (JSONArray) parser.parse(file);
            file.close();
            return obj;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void setInfo(String list,String fileName) {
        BufferedWriter fileWriter = null;
        try {
            fileWriter = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(fileName), "UTF-8"
            ));
            fileWriter.write(list);
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
