import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.File;

public class SendFileFTP {

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

                if (list.get("envoi").equals("1") && list.get("active").equals("1")) {
                    try {
                        session = jsch.getSession("u85460117-upload", "home631778145.1and1-data.host", 22);
                        session.setConfig("StrictHostKeyChecking", "no");
                        session.setPassword("FyK6cIAgpeNOSbEdfrpC*");
                        session.connect();

                        Channel channel = session.openChannel("sftp");
                        channel.connect();
                        ChannelSftp sftpChannel = (ChannelSftp) channel;
                        File dir = new File((String) list.get("path"));
                        File[] directoryListing = dir.listFiles();
                        try {
                            sftpChannel.mkdir((String) list.get("folderftp"));
                        } catch (SftpException e) {
                            System.out.println(e.id); // Prints "Failure"
                            System.out.println(e.getMessage()); // Prints "null"
                            assert (e.id == ChannelSftp.SSH_FX_FAILURE);
                            assert (e.id == 4);
                        }
                        File backup = new File(list.get("path") + "/archive");
                        if (directoryListing != null) {
                            for (File child : directoryListing) {
                                if (child.isFile()) {
                                    sftpChannel.put(child.getAbsolutePath(), "/" + list.get("folderftp") + "/" + child.getName());
                                }
                                // Do something with child
                            }
                        }
                        if (!backup.isDirectory())
                            backup.mkdir();
                        if (directoryListing != null) {
                            for (File child : directoryListing) {
                                if (child.isFile()) {
                                    Table.archiveDocument(list.get("path") + "\\archive", (String) list.get("path"), child.getName());
                                }
                            }
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
