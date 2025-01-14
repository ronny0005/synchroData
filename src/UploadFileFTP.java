import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class UploadFileFTP {
    public static void main(String[] args) {
        FTPClient ftpClient = new FTPClient();
        String databaseSourceFile = "resource/databaseSource.json";
        if (args.length > 0)
            databaseSourceFile = args[0];

        JSONArray listObject = DataBase.getInfoConnexion(databaseSourceFile);
        if (listObject != null) {
            try {
                // Connexion au serveur FTP
                String server = "panel.freehosting.com";
                int port = 21;
                String user = "transfertData@sagelink.com";
                String password = "2fQaqutdltAatxVzyLp9";

                ftpClient.connect(server, port);
                ftpClient.login(user, password);
                ftpClient.enterLocalPassiveMode();
                ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

                System.out.println("Connexion réussie au serveur FTP");

                for (Object o : listObject) {
                    JSONObject list = (JSONObject) o;

                    if (list.get("reception").equals("1") && list.get("active").equals("1")) {
                        String folderFtp = (String) list.get("folderftp");
                        File dir = new File((String) list.get("path"));

                        // Créer le dossier à la racine si nécessaire
                        ftpClient.changeWorkingDirectory("/");
                        if (!ftpClient.changeWorkingDirectory(folderFtp)) {
                            boolean created = ftpClient.makeDirectory(folderFtp);
                            if (created) {
                                System.out.println("Dossier créé : " + folderFtp);
                            } else {
                                System.out.println("Impossible de créer le dossier : " + folderFtp);
                                continue;
                            }
                        }

                        ftpClient.changeWorkingDirectory(folderFtp);

                        // Télécharger les fichiers ZIP et CSV
                        Arrays.asList("*.zip", "*.csv").forEach(extension -> {
                            try {
                                FTPFile[] files = ftpClient.listFiles();
                                for (FTPFile file : files) {
                                    if (file.isFile() && file.getName().endsWith(extension.substring(1))) {
                                        File localFile = new File(dir, file.getName());
                                        try (FileOutputStream fos = new FileOutputStream(localFile)) {
                                            boolean success = ftpClient.retrieveFile(file.getName(), fos);
                                            if (success) {
                                                System.out.println("Fichier téléchargé : " + file.getName());
                                                ftpClient.deleteFile(file.getName());
                                            } else {
                                                System.out.println("Échec du téléchargement : " + file.getName());
                                            }
                                        }
                                    }
                                }
                            } catch (IOException e) {
                                System.out.println("Erreur lors du traitement des fichiers : " + e.getMessage());
                            }
                        });

                        /*
                        // Créer un dossier d'archive local s'il n'existe pas
                        File backup = new File(dir, "archive");
                        if (!backup.isDirectory()) backup.mkdir();

                        // Déplacer les fichiers téléchargés dans le dossier d'archive local
                        File[] downloadedFiles = dir.listFiles();
                        if (downloadedFiles != null) {
                            for (File downloadedFile : downloadedFiles) {
                                if (downloadedFile.isFile()) {
                                    File archivedFile = new File(backup, downloadedFile.getName());
                                    if (downloadedFile.renameTo(archivedFile)) {
                                        System.out.println("Fichier archivé : " + archivedFile.getName());
                                    } else {
                                        System.out.println("Échec de l'archivage : " + downloadedFile.getName());
                                    }
                                }
                            }
                        }*/
                    }
                }

                // Déconnexion du serveur FTP
                ftpClient.logout();
                ftpClient.disconnect();
                System.out.println("Déconnecté du serveur FTP");

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
