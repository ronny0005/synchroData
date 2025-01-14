import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import java.io.*;

public class SendFileSFTP {

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
                        //session = jsch.getSession("u85460117-upload", "home631778145.1and1-data.host", 22);
                        //session = jsch.getSession("u94723657", "217.160.123.210", 22);
                        session = jsch.getSession("transfertData@sagelink.com", "195.201.9.76", 21);

                        session.setPassword("2fQaqutdltAatxVzyLp9");
                        //session.setPassword("ilIBWTvZme4CZD5BzMez");
                        //session.setPassword("FyK6cIAgpeNOSbEdfrpC*");
                        session.setConfig("StrictHostKeyChecking", "no");
                        session.connect();

                        Channel channel = session.openChannel("ftp");
                        channel.connect();
                        ChannelSftp sftpChannel = (ChannelSftp) channel;
                        File dir = new File((String) list.get("path"));

                        File[] directoryListing = dir.listFiles();

                        // Récupérer les dossiers FTP séparés par des virgules
                        String[] folderFtpList = ((String) list.get("folderftp")).split(";");

                        // Itérer sur chaque dossier FTP
                        for (String folderFtp : folderFtpList) {
                            try {
                                sftpChannel.mkdir(folderFtp);  // Crée le dossier sur le serveur FTP
                            } catch (SftpException e) {
                                // Si le dossier existe déjà, on ignore l'exception
                                System.out.println("Dossier déjà présent ou erreur: " + e.getMessage());
                                assert (e.id == ChannelSftp.SSH_FX_FAILURE || e.id == 4);
                            }

                            // Transférer les fichiers vers chaque dossier
                            if (directoryListing != null) {
                                for (File child : directoryListing) {
                                    if (child.isFile()) {
                                        sftpChannel.put(child.getAbsolutePath(), "/" + folderFtp + "/" + child.getName());
                                    }
                                }
                            }
                        }

                        // Créer un dossier d'archive local s'il n'existe pas
                        File backup = new File(list.get("path") + "/archive");

                        if (!backup.isDirectory())
                            backup.mkdir();

                        // Déplacer les fichiers dans le dossier d'archive local
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
