import com.jcraft.jsch.*;
import org.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

public class UploadFileFTP {

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

    public static void main(String[] args){
        JSch jsch = new JSch();
        Session session = null;
        String databaseSourceFile = "resource/databaseDest.json";
        JSONObject list = DataBase.getInfoConnexion(databaseSourceFile);
        try {
            session = jsch.getSession("u85460117-upload", "home631778145.1and1-data.host", 22);
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword("FyK6cIAgpeNOSbEdfrpC*");
            session.connect();

            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftpChannel = (ChannelSftp) channel;
            File dir = new File((String)list.get("path"));
            sftpChannel.cd((String)list.get("folderftp"));

            Vector<ChannelSftp.LsEntry> listFiles = sftpChannel.ls("*.csv");
            for(ChannelSftp.LsEntry entry : listFiles) {
                if(!(dir).isDirectory())
                    dir.mkdir();
                System.out.println(dir.getAbsolutePath()+"\\"+ entry.getFilename());
                sftpChannel.get(entry.getFilename(), dir.getAbsolutePath()+"\\"+ entry.getFilename());
                sftpChannel.rm(entry.getFilename());
            }
            sftpChannel.exit();
            session.disconnect();
        } catch (JSchException e) {
            e.printStackTrace();
        } catch (SftpException e) {
            e.printStackTrace();
        }
    }
}
