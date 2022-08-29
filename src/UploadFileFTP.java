import com.jcraft.jsch.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.File;
import java.util.Vector;

public class UploadFileFTP {

    public static void main(String[] args) {
        JSch jsch = new JSch();
        Session session;
        String databaseSourceFile = "resource/databaseSource.json";
        if (args.length > 0)
            databaseSourceFile = args[0];

        JSONArray listObject = DataBase.getInfoConnexion(databaseSourceFile);
        if (listObject != null) {
            for (Object o : listObject) {
                JSONObject list = (JSONObject) o;

                if (list.get("reception").equals("1") && list.get("active").equals("1")) {
                    try {
                        session = jsch.getSession("u85460117-upload", "home631778145.1and1-data.host", 22);
                        session.setConfig("StrictHostKeyChecking", "no");
                        session.setPassword("FyK6cIAgpeNOSbEdfrpC*");
                        session.connect();

                        Channel channel = session.openChannel("sftp");
                        channel.connect();
                        ChannelSftp sftpChannel = (ChannelSftp) channel;
                        File dir = new File((String) list.get("path"));
                        sftpChannel.cd((String) list.get("folderftp"));

                        Vector<ChannelSftp.LsEntry> listFiles = sftpChannel.ls("*.zip");
                        for (ChannelSftp.LsEntry entry : listFiles) {
                            if (!(dir).isDirectory())
                                dir.mkdir();
                            System.out.println(dir.getAbsolutePath() + "\\" + entry.getFilename());
                            sftpChannel.get(entry.getFilename(), dir.getAbsolutePath() + "\\" + entry.getFilename());
                            sftpChannel.rm(entry.getFilename());
                        }
                        sftpChannel.exit();
                        session.disconnect();
                    } catch (JSchException | SftpException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
