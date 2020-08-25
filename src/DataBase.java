import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class DataBase {

    public static ArrayList<String> getInfoConnexion(String fileName)
    {
        ArrayList<String> list = new ArrayList<String>();
        BufferedReader reader;
        File file = new File(fileName);
        String absolute = file.getAbsolutePath();
        try {
            reader = new BufferedReader(new FileReader(
                    absolute));
            String line = reader.readLine();
            while (line != null) {
                list.add(line);
                // read next line
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    public static void setInfo(ArrayList<String> list,String fileName) {
        BufferedWriter fileWriter = null;
        try {
            fileWriter = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(fileName), "UTF-8"
            ));

            for (String value : list) {
                fileWriter.write(value);
                fileWriter.newLine();
            }
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
