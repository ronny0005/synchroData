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
    private JTextField dossierFTP;
    private JTextField codeAgence;
    private String databaseSourceFile = "resource/databaseSource.csv";
    private String databaseDestFile = "resource/databaseDest.csv";
    private ArrayList<String> infoSource = DataBase.getInfoConnexion(databaseSourceFile);
    private ArrayList<String> infoDest = DataBase.getInfoConnexion(databaseDestFile);

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

        DestServerName.setText(infoDest.get(0));
        DestBaseDeDonnees.setText(infoDest.get(1));
        DestLogin.setText(infoDest.get(2));
        DestMotDePasse.setText(infoDest.get(3));
        DestRepertoire.setText(infoDest.get(4));
        dossierFTP.setText(infoDest.get(18));
        codeAgence.setText(infoDest.get(19));
        checkBox(Integer.valueOf(infoDest.get(5)),DestArticlesCheckBox);
        checkBox(Integer.valueOf(infoDest.get(6)),DestTiersCheckBox);
        checkBox(Integer.valueOf(infoDest.get(7)),DestDepotCheckBox);
        checkBox(Integer.valueOf(infoDest.get(8)),DestLivraisonCheckBox);
        checkBox(Integer.valueOf(infoDest.get(9)),DestEnteteCheckBox);
        checkBox(Integer.valueOf(infoDest.get(10)),DestLigneCheckBox);
        checkBox(Integer.valueOf(infoDest.get(11)),DestReglementCheckBox);
        checkBox(Integer.valueOf(infoDest.get(12)),DestFamilleCheckBox);
        checkBox(Integer.valueOf(infoDest.get(13)),DestArtStockCheckBox);
        checkBox(Integer.valueOf(infoDest.get(14)),DestEcritureCCheckBox);
        checkBox(Integer.valueOf(infoDest.get(15)),DestEcritureACheckBox);
        checkBox(Integer.valueOf(infoDest.get(16)), DestCompteGeneralCheckBox);

        SrcServerName.setText(infoSource.get(0));
        SrcBaseDeDonnees.setText(infoSource.get(1));
        SrcLogin.setText(infoSource.get(2));
        SrcMotDePasse.setText(infoSource.get(3));
        SrcRepertoire.setText(infoSource.get(4));
        checkBox(Integer.valueOf(infoSource.get(5)),SrcArticlesCheckBox);
        checkBox(Integer.valueOf(infoSource.get(6)),SrcTiersCheckBox);
        checkBox(Integer.valueOf(infoSource.get(7)),SrcDepotCheckBox);
        checkBox(Integer.valueOf(infoSource.get(8)),SrcLivraisonCheckBox);
        checkBox(Integer.valueOf(infoSource.get(9)),SrcEnteteCheckBox);
        checkBox(Integer.valueOf(infoSource.get(10)),SrcLigneCheckBox);
        checkBox(Integer.valueOf(infoSource.get(11)),SrcReglementCheckBox);
        checkBox(Integer.valueOf(infoSource.get(12)),SrcFamilleCheckBox);
        checkBox(Integer.valueOf(infoSource.get(13)),SrcArtStockCheckBox);
        checkBox(Integer.valueOf(infoSource.get(14)),SrcEcritureCCheckBox);
        checkBox(Integer.valueOf(infoSource.get(15)),SrcEcritureACheckBox);
        checkBox(Integer.valueOf(infoSource.get(16)), SrcCompteGeneralCheckBox);

    }

    public void setConfigData(){
        infoDest.set(0,DestServerName.getText());
        infoDest.set(1,DestBaseDeDonnees.getText());
        infoDest.set(2,DestLogin.getText());
        infoDest.set(3,DestMotDePasse.getText());
        infoDest.set(4,DestRepertoire.getText());
        infoDest.set(18,dossierFTP.getText());
        infoDest.set(19,codeAgence.getText());
        infoDest.set(5,setCheckBox(DestArticlesCheckBox));
        infoDest.set(6,setCheckBox(DestTiersCheckBox));
        infoDest.set(7,setCheckBox(DestDepotCheckBox));
        infoDest.set(8,setCheckBox(DestLivraisonCheckBox));
        infoDest.set(9,setCheckBox(DestEnteteCheckBox));
        infoDest.set(10,setCheckBox(DestLigneCheckBox));
        infoDest.set(11,setCheckBox(DestReglementCheckBox));
        infoDest.set(12,setCheckBox(DestFamilleCheckBox));
        infoDest.set(13,setCheckBox(DestArtStockCheckBox));
        infoDest.set(14,setCheckBox(DestEcritureCCheckBox));
        infoDest.set(15,setCheckBox(DestEcritureACheckBox));
        infoDest.set(16,setCheckBox(DestCompteGeneralCheckBox));

        infoSource.set(0,SrcServerName.getText());
        infoSource.set(1,SrcBaseDeDonnees.getText());
        infoSource.set(2,SrcLogin.getText());
        infoSource.set(3,SrcMotDePasse.getText());
        infoSource.set(4,SrcRepertoire.getText());
        infoSource.set(18,dossierFTP.getText());
        infoSource.set(19,codeAgence.getText());
        infoSource.set(5,setCheckBox(SrcArticlesCheckBox));
        infoSource.set(6,setCheckBox(SrcTiersCheckBox));
        infoSource.set(7,setCheckBox(SrcDepotCheckBox));
        infoSource.set(8,setCheckBox(SrcLivraisonCheckBox));
        infoSource.set(9,setCheckBox(SrcEnteteCheckBox));
        infoSource.set(10,setCheckBox(SrcLigneCheckBox));
        infoSource.set(11,setCheckBox(SrcReglementCheckBox));
        infoSource.set(12,setCheckBox(SrcFamilleCheckBox));
        infoSource.set(13,setCheckBox(SrcArtStockCheckBox));
        infoSource.set(14,setCheckBox(SrcEcritureCCheckBox));
        infoSource.set(15,setCheckBox(SrcEcritureACheckBox));
        infoSource.set(16,setCheckBox(SrcCompteGeneralCheckBox));
    }

    public ConfigFile(){
        initValue();
        validerButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                setConfigData();
                DataBase.setInfo(infoSource,databaseSourceFile);
                DataBase.setInfo(infoDest,databaseDestFile);

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
