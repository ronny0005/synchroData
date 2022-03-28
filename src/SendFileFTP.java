import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import org.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;

public class SendFileFTP {

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
        String databaseSourceFile = "resource/databaseSource.json";
        if(args.length > 0)
            databaseSourceFile = args[0];
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
            File[] directoryListing = dir.listFiles();
            try {
                sftpChannel.mkdir((String)list.get("folderftp"));
            } catch (SftpException e) {
                System.out.println(e.id); // Prints "Failure"
                System.out.println(e.getMessage()); // Prints "null"
                assert (e.id == ChannelSftp.SSH_FX_FAILURE);
                assert (e.id == 4);
            }
            File backup = new File((String)list.get("path")+"/archive");
            if (directoryListing != null) {
                for (File child : directoryListing) {
                    if(child.isFile()) {
                        sftpChannel.put(child.getAbsolutePath(), "/" + (String)list.get("folderftp") + "/" + child.getName());
                    }
                    // Do something with child
                }
            }
            if(!backup.isDirectory())
                backup.mkdir();
            if (directoryListing != null) {
                for (File child : directoryListing) {
                    if(child.isFile()) {
                        File newFile = new File(backup.getAbsolutePath()+"\\"+child.getName());
                        Table.archiveDocument((String)list.get("path") + "\\archive", (String)list.get("path"), child.getName());
                    }
                }
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
