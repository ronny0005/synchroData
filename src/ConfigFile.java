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
    private JTextField SrcRepertoire;
    private JCheckBox SrcArtStockCheckBox;
    private JCheckBox SrcEcritureCCheckBox;
    private JCheckBox SrcEcritureACheckBox;
    private JCheckBox SrcCompteGeneralCheckBox;
    private JTextField dossierFTPSource;
    private JTextField codeAgence;
    private JCheckBox SrcTaxeCheckBox;
    private JCheckBox factureDeVenteCheckBox;
    private JCheckBox devisCheckBox;
    private JCheckBox bonDeLivraisonCheckBox;
    private JCheckBox factureDAchatCheckBox;
    private JCheckBox documentDEntréeCheckBox;
    private JCheckBox documentInterne1CheckBox;
    private JCheckBox documentDeSortieCheckBox;
    private JCheckBox documentDeTransfertCheckBox;
    private JCheckBox documentInterne2CheckBox;
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

        dossierFTPSource.setText((String)infoSource.get("folderftp"));
        codeAgence.setText((String)infoSource.get("codeagence"));
        SrcServerName.setText((String)infoSource.get("servername"));
        SrcBaseDeDonnees.setText((String)infoSource.get("database"));
        SrcLogin.setText((String)infoSource.get("username"));
        SrcMotDePasse.setText((String)infoSource.get("password"));
        SrcRepertoire.setText((String)infoSource.get("path"));
        checkBox(Integer.valueOf((String)infoSource.get("article")),SrcArticlesCheckBox);
        checkBox(Integer.valueOf((String)infoSource.get("tiers")),SrcTiersCheckBox);
        checkBox(Integer.valueOf((String)infoSource.get("depot")),SrcDepotCheckBox);
        checkBox(Integer.valueOf((String)infoSource.get("livraison")),SrcLivraisonCheckBox);
        checkBox(Integer.valueOf((String)infoSource.get("entete")),SrcEnteteCheckBox);
        checkBox(Integer.valueOf((String)infoSource.get("ligne")),SrcLigneCheckBox);
        checkBox(Integer.valueOf((String)infoSource.get("reglement")),SrcReglementCheckBox);
        checkBox(Integer.valueOf((String)infoSource.get("famille")),SrcFamilleCheckBox);
        checkBox(Integer.valueOf((String)infoSource.get("artstock")),SrcArtStockCheckBox);
        checkBox(Integer.valueOf((String)infoSource.get("ecriturec")),SrcEcritureCCheckBox);
        checkBox(Integer.valueOf((String)infoSource.get("ecriturea")),SrcEcritureACheckBox);
        checkBox(Integer.valueOf((String)infoSource.get("compteg")), SrcCompteGeneralCheckBox);
        checkBox(Integer.valueOf((String)infoSource.get("taxe")), SrcTaxeCheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)infoSource.get("typedocument")).get("facturedevente")), factureDeVenteCheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)infoSource.get("typedocument")).get("devis")), devisCheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)infoSource.get("typedocument")).get("bondelivraison")), bonDeLivraisonCheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)infoSource.get("typedocument")).get("facturedachat")), factureDAchatCheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)infoSource.get("typedocument")).get("entree")), documentDEntréeCheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)infoSource.get("typedocument")).get("sortie")), documentDeSortieCheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)infoSource.get("typedocument")).get("transfert")), documentDeTransfertCheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)infoSource.get("typedocument")).get("documentinterne1")), documentInterne1CheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)infoSource.get("typedocument")).get("documentinterne2")), documentInterne2CheckBox);
    }

    public void setConfigData(){

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
        JSONObject typeDocument = new JSONObject();
        typeDocument.put("facturedevente",setCheckBox(factureDeVenteCheckBox));
        typeDocument.put("devis",setCheckBox(devisCheckBox));
        typeDocument.put("bondelivraison",setCheckBox(bonDeLivraisonCheckBox));
        typeDocument.put("facturedachat",setCheckBox(factureDAchatCheckBox));
        typeDocument.put("entree",setCheckBox(documentDEntréeCheckBox));
        typeDocument.put("sortie",setCheckBox(documentDeSortieCheckBox));
        typeDocument.put("transfert",setCheckBox(documentDeTransfertCheckBox));
        typeDocument.put("documentinterne1",setCheckBox(documentInterne1CheckBox));
        typeDocument.put("documentinterne2",setCheckBox(documentInterne2CheckBox));
        infoSource.put("typedocument",typeDocument);
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
