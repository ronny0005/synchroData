import org.json.simple.JSONObject;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;

public class ConfigFile {
    private JPanel panel1;
    private JCheckBox SrcArticlesCheckBox;
    private JCheckBox SrcTiersCheckBox;
    private JCheckBox SrcDepotCheckBox;
    private JCheckBox SrcFamilleCheckBox;
    private JCheckBox SrcEnteteCheckBox;
    private JCheckBox SrcLivraisonCheckBox;
    private JCheckBox SrcLigneCheckBox;
    private JCheckBox SrcReglementCheckBox;
    private JButton validerButton;
    private JTextField SrcServerName;
    private JTextField SrcBaseDeDonnees;
    private JTextField SrcLogin;
    private JTextField SrcMotDePasse;
    private JTextField DestServerName;
    private JTextField DestBaseDeDonnees;
    private JTextField DestLogin;
    private JTextField DestMotDePasse;
    private JCheckBox DestFamilleCheckBox;
    private JCheckBox DestArticlesCheckBox;
    private JCheckBox DestTiersCheckBox;
    private JCheckBox DestDepotCheckBox;
    private JCheckBox DestLivraisonCheckBox;
    private JCheckBox DestEnteteCheckBox;
    private JCheckBox DestLigneCheckBox;
    private JCheckBox DestReglementCheckBox;
    private JTextField SrcRepertoire;
    private JTextField DestRepertoire;
    private JCheckBox SrcArtStockCheckBox;
    private JCheckBox DestArtStockCheckBox;
    private JCheckBox SrcEcritureCCheckBox;
    private JCheckBox DestEcritureCCheckBox;
    private JCheckBox DestEcritureACheckBox;
    private JCheckBox SrcEcritureACheckBox;
    private JCheckBox SrcCompteGeneralCheckBox;
    private JCheckBox DestCompteGeneralCheckBox;
    private JTextField dossierFTPSource;
    private JTextField codeAgence;
    private JTextField dossierFTPDest;
    private JCheckBox SrcTaxeCheckBox;
    private JCheckBox DestTaxeCheckBox;
    private String databaseSourceFile = "resource/databaseSource.json";
    private String databaseDestFile = "resource/databaseDest.json";
    private JSONObject infoSource = DataBase.getInfoConnexion(databaseSourceFile);
    private JSONObject infoDest = DataBase.getInfoConnexion(databaseDestFile);

    public void checkBox(int value,JCheckBox checkBox){
        if(value == 1)
            checkBox.setSelected(true);
        else
            checkBox.setSelected(false);
    }

    public String setCheckBox(JCheckBox checkBox){
        if(checkBox.isSelected())
            return "1";
        else
            return "0";
    }

    public void initValue(){

        DestServerName.setText((String)infoDest.get("servername"));
        DestBaseDeDonnees.setText((String)infoDest.get("database"));
        DestLogin.setText((String)infoDest.get("username"));
        DestMotDePasse.setText((String)infoDest.get("password"));
        DestRepertoire.setText((String)infoDest.get("path"));
        dossierFTPSource.setText((String)infoSource.get("folderftp"));
        dossierFTPDest.setText((String)infoDest.get("folderftp"));
        codeAgence.setText((String)infoDest.get("codeagence"));
        checkBox(Integer.valueOf((String)infoDest.get("article")),DestArticlesCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("tiers")),DestTiersCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("depot")),DestDepotCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("livraison")),DestLivraisonCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("entete")),DestEnteteCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("ligne")),DestLigneCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("reglement")),DestReglementCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("famille")),DestFamilleCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("artstock")),DestArtStockCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("ecriturec")),DestEcritureCCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("ecriturea")),DestEcritureACheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("compteg")), DestCompteGeneralCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("taxe")), DestTaxeCheckBox);

        SrcServerName.setText((String)infoDest.get("servername"));
        SrcBaseDeDonnees.setText((String)infoSource.get("database"));
        SrcLogin.setText((String)infoDest.get("username"));
        SrcMotDePasse.setText((String)infoDest.get("password"));
        SrcRepertoire.setText((String)infoDest.get("path"));
        checkBox(Integer.valueOf((String)infoDest.get("article")),SrcArticlesCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("tiers")),SrcTiersCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("depot")),SrcDepotCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("livraison")),SrcLivraisonCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("entete")),SrcEnteteCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("ligne")),SrcLigneCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("reglement")),SrcReglementCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("famille")),SrcFamilleCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("artstock")),SrcArtStockCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("ecriturec")),SrcEcritureCCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("ecriturea")),SrcEcritureACheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("compteg")), SrcCompteGeneralCheckBox);
        checkBox(Integer.valueOf((String)infoDest.get("taxe")), SrcTaxeCheckBox);

    }

    public void setConfigData(){
        infoDest.put("servername",DestServerName.getText());
        infoDest.put("database",DestBaseDeDonnees.getText());
        infoDest.put("username",DestLogin.getText());
        infoDest.put("password",DestMotDePasse.getText());
        infoDest.put("path",DestRepertoire.getText());
        infoDest.put("folderftp", dossierFTPDest.getText());
        infoDest.put("codeagence",codeAgence.getText());
        infoDest.put("article",setCheckBox(DestArticlesCheckBox));
        infoDest.put("tiers",setCheckBox(DestTiersCheckBox));
        infoDest.put("depot",setCheckBox(DestDepotCheckBox));
        infoDest.put("livraison",setCheckBox(DestLivraisonCheckBox));
        infoDest.put("entete",setCheckBox(DestEnteteCheckBox));
        infoDest.put("ligne",setCheckBox(DestLigneCheckBox));
        infoDest.put("reglement",setCheckBox(DestReglementCheckBox));
        infoDest.put("famille",setCheckBox(DestFamilleCheckBox));
        infoDest.put("artstock",setCheckBox(DestArtStockCheckBox));
        infoDest.put("ecriturec",setCheckBox(DestEcritureCCheckBox));
        infoDest.put("ecriturea",setCheckBox(DestEcritureACheckBox));
        infoDest.put("compteg",setCheckBox(DestCompteGeneralCheckBox));
        infoDest.put("taxe",setCheckBox(DestTaxeCheckBox));

        infoSource.put("servername",SrcServerName.getText());
        infoSource.put("database",SrcBaseDeDonnees.getText());
        infoSource.put("username",SrcLogin.getText());
        infoSource.put("password",SrcMotDePasse.getText());
        infoSource.put("path",SrcRepertoire.getText());
        infoSource.put("folderftp", dossierFTPSource.getText());
        infoSource.put("codeagence",codeAgence.getText());
        infoSource.put("article",setCheckBox(SrcArticlesCheckBox));
        infoSource.put("tiers",setCheckBox(SrcTiersCheckBox));
        infoSource.put("depot",setCheckBox(SrcDepotCheckBox));
        infoSource.put("livraison",setCheckBox(SrcLivraisonCheckBox));
        infoSource.put("entete",setCheckBox(SrcEnteteCheckBox));
        infoSource.put("ligne",setCheckBox(SrcLigneCheckBox));
        infoSource.put("reglement",setCheckBox(SrcReglementCheckBox));
        infoSource.put("famille",setCheckBox(SrcFamilleCheckBox));
        infoSource.put("artstock",setCheckBox(SrcArtStockCheckBox));
        infoSource.put("ecriturec",setCheckBox(SrcEcritureCCheckBox));
        infoSource.put("ecriturea",setCheckBox(SrcEcritureACheckBox));
        infoSource.put("compteg",setCheckBox(SrcCompteGeneralCheckBox));
        infoSource.put("taxe",setCheckBox(SrcTaxeCheckBox));
    }

    public ConfigFile(){
        initValue();
        validerButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                setConfigData();
                DataBase.setInfo((String)infoSource.toJSONString(),databaseSourceFile);
                DataBase.setInfo((String)infoDest.toJSONString(),databaseDestFile);

                JOptionPane.showMessageDialog(null, "Les informations ont été enregistrées");
            }
        });
    }

    public static void main(String[] args){
        JFrame frame = new JFrame("Config");
        frame.setContentPane(new ConfigFile().panel1);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);



    }
}
