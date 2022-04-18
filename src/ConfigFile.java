import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import javax.swing.*;
import javax.swing.filechooser.FileSystemView;
import java.awt.event.*;
import java.io.File;

public class ConfigFile {
    private JPanel panel1;
    private JCheckBox SrcArticlesCheckBox;
    private JCheckBox SrcTiersCheckBox;
    private JCheckBox SrcDepotCheckBox;
    private JCheckBox SrcEnteteCheckBox;
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
    private JCheckBox venteCheckBox;
    private JCheckBox stockCheckBox;
    private JCheckBox SrcJournauxCheckBox;
    private JCheckBox SrcCollaborateurCheckBox;
    private JTextField SrcNomConfig;
    private JComboBox SrcListConfig;
    private JButton SrcAjoutConfig;
    private JCheckBox EnvoiCheckBox;
    private JCheckBox ReceptionCheckBox;
    private String databaseSourceFile = "resource/databaseSource.json";
    private JSONArray infoSource = DataBase.getInfoConnexion(databaseSourceFile);
    private JSONObject object = (JSONObject) infoSource.get(0);
    private ComboItem comboItem;


    public void fileChooser(){
        JFileChooser choose = new JFileChooser(
                FileSystemView
                        .getFileSystemView()
                        .getHomeDirectory()
        );
        if(!SrcRepertoire.getText().toString().equals(""))
            choose.setCurrentDirectory(new File(SrcRepertoire.getText().toString()));
        choose.setDialogTitle("Choisissez un repertoire: ");
        choose.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        int res = choose.showSaveDialog(null);
        if(res == JFileChooser.APPROVE_OPTION)
        {
            if(choose.getSelectedFile().isDirectory())
            {
                SrcRepertoire.setText(choose.getSelectedFile().toString());
            }
        }
    }

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

    public void setListConfig(){
        SrcListConfig.removeAllItems();
        for(int i = 0;i<infoSource.size();i++)
            SrcListConfig.addItem(new ComboItem((String)((JSONObject)infoSource.get(i)).get("nomconfig"), i+""));

    }
    public void initValue(){
        setListConfig();
        getConfigData(0);
        comboItem = (ComboItem) SrcListConfig.getSelectedItem();
    }

    public void getConfigData(int position) {
        object = (JSONObject) infoSource.get(position);
        dossierFTPSource.setText((String)object.get("folderftp"));
        codeAgence.setText((String)object.get("codeagence"));
        SrcServerName.setText((String)object.get("servername"));
        SrcBaseDeDonnees.setText((String)object.get("database"));
        SrcLogin.setText((String)object.get("username"));
        SrcMotDePasse.setText((String)object.get("password"));
        SrcRepertoire.setText((String)object.get("path"));
        SrcNomConfig.setText((String)object.get("nomconfig"));
        checkBox(Integer.valueOf((String)object.get("envoi")),EnvoiCheckBox);
        checkBox(Integer.valueOf((String)object.get("reception")),ReceptionCheckBox);
        checkBox(Integer.valueOf((String)object.get("article")),SrcArticlesCheckBox);
        checkBox(Integer.valueOf((String)object.get("tiers")),SrcTiersCheckBox);
        checkBox(Integer.valueOf((String)object.get("depot")),SrcDepotCheckBox);
        checkBox(Integer.valueOf((String)object.get("entete")),SrcEnteteCheckBox);
        checkBox(Integer.valueOf((String)object.get("ligne")),SrcLigneCheckBox);
        checkBox(Integer.valueOf((String)object.get("reglement")),SrcReglementCheckBox);
        checkBox(Integer.valueOf((String)object.get("artstock")),SrcArtStockCheckBox);
        checkBox(Integer.valueOf((String)object.get("ecriturec")),SrcEcritureCCheckBox);
        checkBox(Integer.valueOf((String)object.get("ecriturea")),SrcEcritureACheckBox);
        checkBox(Integer.valueOf((String)object.get("compteg")), SrcCompteGeneralCheckBox);
        checkBox(Integer.valueOf((String)object.get("taxe")), SrcTaxeCheckBox);
        checkBox(Integer.valueOf((String)object.get("journaux")), SrcJournauxCheckBox);
        checkBox(Integer.valueOf((String)object.get("collaborateur")), SrcCollaborateurCheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)object.get("typedocument")).get("facturedevente")), factureDeVenteCheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)object.get("typedocument")).get("devis")), devisCheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)object.get("typedocument")).get("bondelivraison")), bonDeLivraisonCheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)object.get("typedocument")).get("facturedachat")), factureDAchatCheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)object.get("typedocument")).get("entree")), documentDEntréeCheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)object.get("typedocument")).get("sortie")), documentDeSortieCheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)object.get("typedocument")).get("transfert")), documentDeTransfertCheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)object.get("typedocument")).get("documentinterne1")), documentInterne1CheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)object.get("typedocument")).get("documentinterne2")), documentInterne2CheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)object.get("typedocument")).get("vente")), venteCheckBox);
        checkBox(Integer.valueOf((String)((JSONObject)object.get("typedocument")).get("stock")), stockCheckBox);
    }

    public JSONObject initJson(){
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("servername","");
        jsonObject.put("nomconfig","");
        jsonObject.put("database","");
        jsonObject.put("username","");
        jsonObject.put("password","");
        jsonObject.put("path","");
        jsonObject.put("folderftp", "");
        jsonObject.put("codeagence","");
        jsonObject.put("article","0");
        jsonObject.put("tiers","0");
        jsonObject.put("depot","0");
        jsonObject.put("entete","0");
        jsonObject.put("ligne","0");
        jsonObject.put("reglement","0");
        jsonObject.put("artstock","0");
        jsonObject.put("ecriturec","0");
        jsonObject.put("ecriturea","0");
        jsonObject.put("compteg","0");
        jsonObject.put("taxe","0");
        jsonObject.put("journaux","0");
        jsonObject.put("collaborateur","0");
        JSONObject typeDocument = new JSONObject();
        typeDocument.put("facturedevente","0");
        typeDocument.put("devis","0");
        typeDocument.put("bondelivraison","0");
        typeDocument.put("facturedachat","0");
        typeDocument.put("entree","0");
        typeDocument.put("sortie","0");
        typeDocument.put("transfert","0");
        typeDocument.put("documentinterne1","0");
        typeDocument.put("documentinterne2","0");
        typeDocument.put("vente","0");
        typeDocument.put("stock","0");
        typeDocument.put("envoi","0");
        typeDocument.put("reception","0");
        jsonObject.put("typedocument",typeDocument);
        return jsonObject;
    }
    public void setConfigData(int position){
        object = (JSONObject) infoSource.get(position);
        object.put("servername",SrcServerName.getText());
        object.put("nomconfig",SrcNomConfig.getText());
        object.put("database",SrcBaseDeDonnees.getText());
        object.put("username",SrcLogin.getText());
        object.put("password",SrcMotDePasse.getText());
        object.put("path",SrcRepertoire.getText());
        object.put("folderftp", dossierFTPSource.getText());
        object.put("codeagence",codeAgence.getText());
        object.put("article",setCheckBox(SrcArticlesCheckBox));
        object.put("tiers",setCheckBox(SrcTiersCheckBox));
        object.put("depot",setCheckBox(SrcDepotCheckBox));
        object.put("entete",setCheckBox(SrcEnteteCheckBox));
        object.put("ligne",setCheckBox(SrcLigneCheckBox));
        object.put("reglement",setCheckBox(SrcReglementCheckBox));
        object.put("artstock",setCheckBox(SrcArtStockCheckBox));
        object.put("ecriturec",setCheckBox(SrcEcritureCCheckBox));
        object.put("ecriturea",setCheckBox(SrcEcritureACheckBox));
        object.put("compteg",setCheckBox(SrcCompteGeneralCheckBox));
        object.put("taxe",setCheckBox(SrcTaxeCheckBox));
        object.put("journaux",setCheckBox(SrcJournauxCheckBox));
        object.put("collaborateur",setCheckBox(SrcCollaborateurCheckBox));
        object.put("envoi",setCheckBox(EnvoiCheckBox));
        object.put("reception",setCheckBox(ReceptionCheckBox));
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
        typeDocument.put("vente",setCheckBox(venteCheckBox));
        typeDocument.put("stock",setCheckBox(stockCheckBox));
        object.put("typedocument",typeDocument);
        infoSource.set(position,object);
    }

    public ConfigFile(){
        initValue();
        validerButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                DataBase.setInfo((String)infoSource.toJSONString(),databaseSourceFile);

                JOptionPane.showMessageDialog(null, "Les informations ont été enregistrées");
            }
        });
        venteCheckBox.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                if(e.getStateChange() == ItemEvent.SELECTED) {
                    factureDeVenteCheckBox.setSelected(true);
                    devisCheckBox.setSelected(true);
                    bonDeLivraisonCheckBox.setSelected(true);
                } else {
                    factureDeVenteCheckBox.setSelected(false);
                    devisCheckBox.setSelected(false);
                    bonDeLivraisonCheckBox.setSelected(false);
                };
            }
        });
        factureDeVenteCheckBox.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                if(e.getStateChange() != ItemEvent.SELECTED) {
                    venteCheckBox.setSelected(false);
                } else {

                };
            }
        });
        bonDeLivraisonCheckBox.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                if(e.getStateChange() != ItemEvent.SELECTED) {
                    venteCheckBox.setSelected(false);
                } else {

                };
            }
        });

        stockCheckBox.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                if(e.getStateChange() == ItemEvent.SELECTED) {
                    documentDEntréeCheckBox.setSelected(true);
                    documentDeSortieCheckBox.setSelected(true);
                    documentDeTransfertCheckBox.setSelected(true);
                } else {
                    documentDEntréeCheckBox.setSelected(false);
                    documentDeSortieCheckBox.setSelected(false);
                    documentDeTransfertCheckBox.setSelected(false);
                };
            }
        });

        documentDEntréeCheckBox.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                if(e.getStateChange() != ItemEvent.SELECTED) {
                    stockCheckBox.setSelected(false);
                } else {

                };
            }
        });

        documentDeSortieCheckBox.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                if(e.getStateChange() != ItemEvent.SELECTED) {
                    stockCheckBox.setSelected(false);
                } else {

                };
            }
        });

        documentDeTransfertCheckBox.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                if(e.getStateChange() != ItemEvent.SELECTED) {
                    stockCheckBox.setSelected(false);
                } else {

                };
            }
        });

        ReceptionCheckBox.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                if(e.getStateChange() == ItemEvent.SELECTED) {
                    EnvoiCheckBox.setSelected(false);
                    setConfigData(Integer.valueOf(comboItem.getValue()));
                } else {

                };
            }
        });

        EnvoiCheckBox.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                if(e.getStateChange() == ItemEvent.SELECTED) {
                    ReceptionCheckBox.setSelected(false);
                    setConfigData(Integer.valueOf(comboItem.getValue()));
                } else {

                };
            }
        });

        SrcRepertoire.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                fileChooser();
            }
        });

        SrcAjoutConfig.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                infoSource.add(initJson());
                setListConfig();
                getConfigData(infoSource.size()-1);
                SrcListConfig.setSelectedIndex(SrcListConfig.getItemCount()-1);
                comboItem = (ComboItem) SrcListConfig.getSelectedItem();
            }
        });

        SrcListConfig.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                getConfigData(Integer.valueOf(((ComboItem)e.getItem()).getValue()));
                comboItem = (ComboItem) SrcListConfig.getSelectedItem();
            }
        });

        SrcNomConfig.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
                setListConfig();
            }
        });

        SrcServerName.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        SrcBaseDeDonnees.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        SrcRepertoire.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        dossierFTPSource.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        codeAgence.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        SrcLogin.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        SrcMotDePasse.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        SrcArticlesCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        SrcTiersCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        SrcEnteteCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        SrcDepotCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        SrcLigneCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        SrcReglementCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        SrcArtStockCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        SrcEcritureCCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        SrcEcritureACheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        SrcCompteGeneralCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        SrcTaxeCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        SrcJournauxCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        SrcCollaborateurCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        venteCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        factureDeVenteCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        devisCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        bonDeLivraisonCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        factureDAchatCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        stockCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        documentDEntréeCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        documentDeSortieCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        documentDeTransfertCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        documentInterne1CheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
            }
        });

        documentInterne2CheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setConfigData(Integer.valueOf(comboItem.getValue()));
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

    public void setTaxe(int valSelect){
        if(valSelect == 1){
            SrcCompteGeneralCheckBox.setSelected(true);
            SrcTaxeCheckBox.setSelected(true);
        }else{
            SrcTaxeCheckBox.setSelected(false);
        }
    }

    public void setJournaux(int valSelect){
        if(valSelect == 1){
            SrcCompteGeneralCheckBox.setSelected(true);
            SrcJournauxCheckBox.setSelected(true);
        }else{
            SrcJournauxCheckBox.setSelected(false);
        }
    }

    public void setCompteTiers(int valSelect){
        if(valSelect == 1){
            SrcCompteGeneralCheckBox.setSelected(true);
            SrcTiersCheckBox.setSelected(true);
        }else{
            SrcTiersCheckBox.setSelected(false);
        }
    }

    public void setArticle(int valSelect){
        if(valSelect == 1){
            SrcArticlesCheckBox.setSelected(true);
        }else{
            SrcArticlesCheckBox.setSelected(false);
        }
    }

    public void setArtStock(int valSelect){
        if(valSelect == 1){
            setArticle(1);
            SrcDepotCheckBox.setSelected(true);
        }else{
            setArticle(0);
            SrcDepotCheckBox.setSelected(false);
        }
    }

    public void setEntete(int valSelect){
        if(valSelect == 1){
            setArtStock(1);
            setCompteTiers(1);
            setJournaux(1);
            setTaxe(1);
        }else{
            setArticle(0);
            setCompteTiers(0);
            setJournaux(0);
            setTaxe(0);
        }
    }

    public void setLigne(int valSelect){
        if(valSelect == 1)
        {
            setEntete(1);
            SrcLigneCheckBox.setSelected(true);
        }
        else{
            setEntete(0);
            SrcLigneCheckBox.setSelected(false);
        }
    }

    class ComboItem
    {
        private String key;
        private String value;

        public ComboItem(String key, String value)
        {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString()
        {
            return key;
        }

        public String getKey()
        {
            return key;
        }

        public String getValue()
        {
            return value;
        }
    }

}
