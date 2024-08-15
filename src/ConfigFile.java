import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import javax.swing.*;
import javax.swing.filechooser.FileSystemView;
import java.awt.event.*;
import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

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
    private JCheckBox SrcTarifCondCheckBox;
    private JCheckBox SrcEnumCondCheckBox;
    private JCheckBox SrcPConditionnementCheckBox;
    private JCheckBox SrcPUniteCheckBox;
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
    private JComboBox<ComboItem> SrcListConfig;
    private JButton SrcAjoutConfig;
    private JCheckBox EnvoiCheckBox;
    private JCheckBox ReceptionCheckBox;
    private JFormattedTextField dateMajConfig;
    private JCheckBox ActiveCheckBox;
    private JScrollPane panelScroll;
    private final String databaseSourceFile = "resource/databaseSource.json";
    private final JSONArray infoSource = DataBase.getInfoConnexion(databaseSourceFile);
    private JSONObject object = (JSONObject) (infoSource != null ? infoSource.get(0) : null);
    private ComboItem comboItem;
    private boolean newItem;
    private boolean focusOutName;


    public void fileChooser(){
        JFileChooser choose = new JFileChooser(
                FileSystemView
                        .getFileSystemView()
                        .getHomeDirectory()
        );
        if(!SrcRepertoire.getText().equals(""))
            choose.setCurrentDirectory(new File(SrcRepertoire.getText()));
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
        checkBox.setSelected(value == 1);
    }

    public String setCheckBox(JCheckBox checkBox){
        if(checkBox.isSelected())
            return "1";
        else
            return "0";
    }

    public void setListConfig(boolean ajout){
        int selected = ((SrcListConfig.getSelectedIndex() == -1) ? 0 : SrcListConfig.getSelectedIndex());
        if(ajout)
            if (infoSource != null) {
                selected = infoSource.size()-1;
            }
        SrcListConfig.removeAllItems();
        String nomConfig;
        JSONObject object;
        if (infoSource != null) {
            for(int i = 0;i<infoSource.size();i++) {
                object = ((JSONObject) infoSource.get(i));
                nomConfig = (String) (object.get("nomconfig"));
                ComboItem comboItemAdd  = new ComboItem(nomConfig, i + "");
                if(i==selected)
                    comboItem = comboItemAdd;
                SrcListConfig.addItem(comboItemAdd);
            }
        }
        SrcListConfig.setSelectedIndex(selected);
    }

    public void AddItem(){
        if (infoSource != null) {
            SrcListConfig.addItem(new ComboItem("", (infoSource.size() - 1) + ""));
        }
    }

    public void initValue(){
        newItem = false;
        setListConfig(false);
        getConfigData(0);
        comboItem = SrcListConfig.getItemAt(0);
    }

    public String GetdateFormat(String inputDateStr){
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat outputFormat = new SimpleDateFormat("dd/MM/yyyy");

        Date date = null;
        String outputDateStr;
        try {
            date = inputFormat.parse(inputDateStr);
            outputDateStr = outputFormat.format(date);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return outputDateStr;
    }

    public String ReturnDateFormat(String inputDateStr){
        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat inputFormat = new SimpleDateFormat("dd/MM/yyyy");

        Date date = null;
        String outputDateStr;
        try {
            date = inputFormat.parse(inputDateStr);
            outputDateStr = outputFormat.format(date);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return outputDateStr;
    }
    public void getConfigData(int position) {
        object = (JSONObject) (infoSource != null ? infoSource.get(position) : null);
        if (object != null) {
            dossierFTPSource.setText((String)object.get("folderftp"));
            codeAgence.setText((String)object.get("codeagence"));
            SrcServerName.setText((String)object.get("servername"));
            SrcBaseDeDonnees.setText((String)object.get("database"));
            SrcLogin.setText((String)object.get("username"));
            SrcMotDePasse.setText((String)object.get("password"));
            SrcRepertoire.setText((String)object.get("path"));
            SrcNomConfig.setText((String)object.get("nomconfig"));
            String dateMaj = ReturnDateFormat( DateTimeFormatter.ofPattern("dd/MM/yyyy").format(LocalDate.now()));
            if((object.get("datemaj")) != null)
                dateMaj = ((String)object.get("datemaj"));
            dateMajConfig.setText(GetdateFormat(dateMaj));

            checkBox(Integer.parseInt((String)object.get("envoi")),EnvoiCheckBox);
            checkBox(Integer.parseInt((String)object.get("active")),ActiveCheckBox);
            checkBox(Integer.parseInt((String)object.get("reception")),ReceptionCheckBox);
            checkBox(Integer.parseInt((String)object.get("article")),SrcArticlesCheckBox);
            checkBox(Integer.parseInt((String)object.get("tiers")),SrcTiersCheckBox);
            checkBox(Integer.parseInt((String)object.get("depot")),SrcDepotCheckBox);
            checkBox(Integer.parseInt((String)object.get("entete")),SrcEnteteCheckBox);
            checkBox(Integer.parseInt((String)object.get("ligne")),SrcLigneCheckBox);
            checkBox(Integer.parseInt((String)object.get("reglement")),SrcReglementCheckBox);
            checkBox(Integer.parseInt((String)object.get("artstock")),SrcArtStockCheckBox);
            checkBox(Integer.parseInt((String)object.get("ecriturec")),SrcEcritureCCheckBox);
            checkBox(Integer.parseInt((String)object.get("ecriturea")),SrcEcritureACheckBox);
            checkBox(Integer.parseInt((String)object.get("compteg")), SrcCompteGeneralCheckBox);
            checkBox(Integer.parseInt((String)object.get("taxe")), SrcTaxeCheckBox);
            checkBox(Integer.parseInt((String)object.get("journaux")), SrcJournauxCheckBox);
            checkBox(Integer.parseInt((String)object.get("collaborateur")), SrcCollaborateurCheckBox);
            /*
            checkBox(Integer.parseInt((String)object.get("ftarifcond")), SrcTarifCondCheckBox);
            checkBox(Integer.parseInt((String)object.get("fenumcond")), SrcEnumCondCheckBox);
            checkBox(Integer.parseInt((String)object.get("pconditionnement")), SrcPConditionnementCheckBox);
            checkBox(Integer.parseInt((String)object.get("punite")), SrcPUniteCheckBox);
             */
            checkBox(Integer.parseInt((String)((JSONObject)object.get("typedocument")).get("facturedevente")), factureDeVenteCheckBox);
            checkBox(Integer.parseInt((String)((JSONObject)object.get("typedocument")).get("devis")), devisCheckBox);
            checkBox(Integer.parseInt((String)((JSONObject)object.get("typedocument")).get("bondelivraison")), bonDeLivraisonCheckBox);
            checkBox(Integer.parseInt((String)((JSONObject)object.get("typedocument")).get("facturedachat")), factureDAchatCheckBox);
            checkBox(Integer.parseInt((String)((JSONObject)object.get("typedocument")).get("entree")), documentDEntréeCheckBox);
            checkBox(Integer.parseInt((String)((JSONObject)object.get("typedocument")).get("sortie")), documentDeSortieCheckBox);
            checkBox(Integer.parseInt((String)((JSONObject)object.get("typedocument")).get("transfert")), documentDeTransfertCheckBox);
            checkBox(Integer.parseInt((String)((JSONObject)object.get("typedocument")).get("documentinterne1")), documentInterne1CheckBox);
            checkBox(Integer.parseInt((String)((JSONObject)object.get("typedocument")).get("documentinterne2")), documentInterne2CheckBox);
            checkBox(Integer.parseInt((String)((JSONObject)object.get("typedocument")).get("vente")), venteCheckBox);
            checkBox(Integer.parseInt((String)((JSONObject)object.get("typedocument")).get("stock")), stockCheckBox);
        }
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
        jsonObject.put("envoi","0");
        jsonObject.put("active","1");
        jsonObject.put("reception","0");
        jsonObject.put("ftarifcond","0");
        jsonObject.put("fenumcond","0");
        jsonObject.put("pconditionnement","1");
        jsonObject.put("punite","0");
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
        typeDocument.put("datemaj",ReturnDateFormat( DateTimeFormatter.ofPattern("dd/MM/yyyy").format(LocalDate.now())));
        jsonObject.put("typedocument",typeDocument);
        return jsonObject;
    }
    public void setConfigData(int position){
        if (infoSource != null) {
            object = (JSONObject) infoSource.get(position);
            object.put("servername",SrcServerName.getText());
            object.put("nomconfig",SrcNomConfig.getText());
            object.put("database",SrcBaseDeDonnees.getText());
            object.put("username",SrcLogin.getText());
            object.put("datemaj",ReturnDateFormat(dateMajConfig.getText()));
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
            object.put("ftarifcond",setCheckBox(SrcTarifCondCheckBox));
            object.put("fenumcond",setCheckBox(SrcEnumCondCheckBox));
            object.put("pconditionnement",setCheckBox(SrcPConditionnementCheckBox));
            object.put("punite",setCheckBox(SrcPUniteCheckBox));
            object.put("compteg",setCheckBox(SrcCompteGeneralCheckBox));
            object.put("taxe",setCheckBox(SrcTaxeCheckBox));
            object.put("journaux",setCheckBox(SrcJournauxCheckBox));
            object.put("collaborateur",setCheckBox(SrcCollaborateurCheckBox));
            object.put("envoi",setCheckBox(EnvoiCheckBox));
            object.put("active",setCheckBox(ActiveCheckBox));
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
    }

    public ConfigFile(){
        initValue();
        validerButton.addActionListener(actionEvent -> {
            if (infoSource != null) {
                setConfigData(Integer.parseInt(comboItem.getValue()));
                DataBase.setInfo(infoSource.toJSONString(),databaseSourceFile);
            }

            JOptionPane.showMessageDialog(null, "Les informations ont été enregistrées");
        });
        venteCheckBox.addItemListener(e -> {
            if(e.getStateChange() == ItemEvent.SELECTED) {
                factureDeVenteCheckBox.setSelected(true);
                devisCheckBox.setSelected(true);
                bonDeLivraisonCheckBox.setSelected(true);
            } else {
                factureDeVenteCheckBox.setSelected(false);
                devisCheckBox.setSelected(false);
                bonDeLivraisonCheckBox.setSelected(false);
            }
        });
        factureDeVenteCheckBox.addItemListener(e -> {
            if(e.getStateChange() != ItemEvent.SELECTED) {
                venteCheckBox.setSelected(false);
            }
        });
        bonDeLivraisonCheckBox.addItemListener(e -> {
            if(e.getStateChange() != ItemEvent.SELECTED) {
                venteCheckBox.setSelected(false);
            }
        });

        stockCheckBox.addItemListener(e -> {
            if(e.getStateChange() == ItemEvent.SELECTED) {
                documentDEntréeCheckBox.setSelected(true);
                documentDeSortieCheckBox.setSelected(true);
                documentDeTransfertCheckBox.setSelected(true);
            } else {
                documentDEntréeCheckBox.setSelected(false);
                documentDeSortieCheckBox.setSelected(false);
                documentDeTransfertCheckBox.setSelected(false);
            }
        });

        documentDEntréeCheckBox.addItemListener(e -> {
            if(e.getStateChange() != ItemEvent.SELECTED) {
                stockCheckBox.setSelected(false);
            }
        });

        documentDeSortieCheckBox.addItemListener(e -> {
            if(e.getStateChange() != ItemEvent.SELECTED) {
                stockCheckBox.setSelected(false);
            }
        });

        documentDeTransfertCheckBox.addItemListener(e -> {
            if(e.getStateChange() != ItemEvent.SELECTED) {
                stockCheckBox.setSelected(false);
            }
        });

        ReceptionCheckBox.addItemListener(e -> {
            if(e.getStateChange() == ItemEvent.SELECTED) {
                EnvoiCheckBox.setSelected(false);
                if(comboItem != null) {
                    int value = Integer.parseInt(comboItem.getValue());
                    setConfigData(value);
                }
            }
        });

        EnvoiCheckBox.addItemListener(e -> {
            if(e.getStateChange() == ItemEvent.SELECTED) {
                ReceptionCheckBox.setSelected(false);
                setConfigData(Integer.parseInt(comboItem.getValue()));
            }
        });

        ActiveCheckBox.addItemListener(e -> {
            int value = Integer.parseInt(comboItem.getValue());
            setConfigData(value);
        });

        SrcRepertoire.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                fileChooser();
            }
        });

        SrcAjoutConfig.addActionListener(e -> {
            if (infoSource != null) {
                newItem = true;
                infoSource.add(initJson());
                AddItem();
                setListConfig(true);
                getConfigData(infoSource.size()-1);
                newItem = false;
            }
        });

        SrcListConfig.addActionListener(e -> {
            if(!focusOutName)
                comboItem = SrcListConfig.getItemAt(SrcListConfig.getSelectedIndex());
            if(!newItem) {
                getConfigData(Integer.parseInt(comboItem.getValue()));
                setListConfig(false);
            }
        });

        SrcNomConfig.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                focusOutName = true;
                setConfigData(Integer.parseInt(comboItem.getValue()));
                if (infoSource != null) {
                    object = ((JSONObject) infoSource.get(Integer.parseInt(comboItem.getValue())));
                }
                setListConfig(false);
                focusOutName = false;
            }
        });

        SrcServerName.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                setConfigData(Integer.parseInt(comboItem.getValue()));
            }
        });

        SrcBaseDeDonnees.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                setConfigData(Integer.parseInt(comboItem.getValue()));
            }
        });

        SrcRepertoire.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                setConfigData(Integer.parseInt(comboItem.getValue()));
            }
        });

        dossierFTPSource.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                setConfigData(Integer.parseInt(comboItem.getValue()));
            }
        });

        codeAgence.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                setConfigData(Integer.parseInt(comboItem.getValue()));
            }
        });

        SrcLogin.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                setConfigData(Integer.parseInt(comboItem.getValue()));
            }
        });

        SrcMotDePasse.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                setConfigData(Integer.parseInt(comboItem.getValue()));
            }
        });

        SrcArticlesCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        SrcTiersCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        SrcEnteteCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        SrcDepotCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        SrcLigneCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        SrcReglementCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        SrcArtStockCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        SrcEcritureCCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        SrcEcritureACheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        SrcCompteGeneralCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        SrcTaxeCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        SrcJournauxCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        SrcCollaborateurCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        venteCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        factureDeVenteCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        devisCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        bonDeLivraisonCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        factureDAchatCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        stockCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        documentDEntréeCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        documentDeSortieCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        documentDeTransfertCheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        documentInterne1CheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

        documentInterne2CheckBox.addActionListener(e -> setConfigData(Integer.parseInt(comboItem.getValue())));

    }

    public static void main(String[] args){
        JFrame frame = new JFrame("Config");
        frame.setContentPane(new ConfigFile().panelScroll);
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
        SrcArticlesCheckBox.setSelected(valSelect == 1);
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

    private void createUIComponents() {
        // TODO: place custom component creation code here
    }

    static class ComboItem
    {
        private final String key;
        private final String value;

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
