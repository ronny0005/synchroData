import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class SendFileFTP {
    public static void main(String[] args) {
        FTPClient ftpClient = new FTPClient();
        String databaseSourceFile = "resource/databaseSource.json";
        if (args.length > 0)
            databaseSourceFile = args[0];

        JSONArray listObject = DataBase.getInfoConnexion(databaseSourceFile);
        if (listObject != null) {
            for (Object o : listObject) {
                JSONObject list = (JSONObject) o;

                if (list.get("envoi").equals("1") && list.get("active").equals("1")) {
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

                        File dir = new File((String) list.get("path"));
                        File[] directoryListing = dir.listFiles();

                        // Récupérer les dossiers FTP séparés par des virgules
                        String[] folderFtpList = ((String) list.get("folderftp")).split(";");

                        // Créer les dossiers à la racine du serveur FTP si nécessaire
                        for (String folderFtp : folderFtpList) {
                            try {
                                // Naviguer à la racine
                                ftpClient.changeWorkingDirectory("/");

                                if (!ftpClient.changeWorkingDirectory(folderFtp)) {
                                    boolean created = ftpClient.makeDirectory(folderFtp);
                                    if (created) {
                                        System.out.println("Dossier créé à la racine : " + folderFtp);
                                    } else {
                                        System.out.println("Échec de la création du dossier à la racine : " + folderFtp);
                                    }
                                }
                                ftpClient.changeWorkingDirectory(folderFtp);

                                // Transférer les fichiers dans chaque dossier
                                if (directoryListing != null) {
                                    for (File child : directoryListing) {
                                        if (child.isFile()) {
                                            try (FileInputStream fis = new FileInputStream(child)) {
                                                boolean done = ftpClient.storeFile(child.getName(), fis);
                                                if (done) {
                                                    System.out.println("Fichier transféré : " + child.getName());
                                                } else {
                                                    System.out.println("Échec du transfert : " + child.getName());
                                                }
                                            }
                                        }
                                    }
                                }

                            } catch (IOException e) {
                                System.out.println("Erreur lors de l'accès au dossier FTP : " + e.getMessage());
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
    }
}
