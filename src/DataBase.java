import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class DataBase {

    public static JSONArray getInfoConnexion(String fileName)
    {
        try {
            JSONParser parser = new JSONParser();
            FileReader file = new FileReader(fileName);
            JSONArray obj = (JSONArray) parser.parse(file);
            file.close();
            return obj;
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void setInfo(String list,String fileName) {
        BufferedWriter fileWriter;
        try {
            fileWriter = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(fileName), StandardCharsets.UTF_8
            ));
            fileWriter.write(list);
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
