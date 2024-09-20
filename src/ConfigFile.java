import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import javax.swing.*;
import javax.swing.filechooser.FileSystemView;
import java.awt.event.*;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ConfigFile {
    private JPanel panel1;
    private JCheckBox SrcArticlesCheckBox, SrcTiersCheckBox, SrcDepotCheckBox, SrcEnteteCheckBox, SrcLigneCheckBox;
    private JCheckBox SrcReglementCheckBox, SrcArtStockCheckBox, SrcEcritureCCheckBox, SrcEcritureACheckBox;
    private JCheckBox SrcTarifCondCheckBox, SrcEnumCondCheckBox, SrcPConditionnementCheckBox, SrcPUniteCheckBox;
    private JCheckBox SrcCompteGeneralCheckBox, SrcTaxeCheckBox, factureDeVenteCheckBox, devisCheckBox, bonDeLivraisonCheckBox;
    private JCheckBox factureDAchatCheckBox, documentDEntréeCheckBox, documentDeSortieCheckBox, documentDeTransfertCheckBox;
    private JCheckBox documentInterne1CheckBox, documentInterne2CheckBox, venteCheckBox, stockCheckBox, SrcJournauxCheckBox;
    private JCheckBox SrcCollaborateurCheckBox, EnvoiCheckBox, ReceptionCheckBox, ActiveCheckBox;
    private JTextField SrcServerName, SrcBaseDeDonnees, SrcLogin, SrcMotDePasse, SrcRepertoire, dossierFTPSource, codeAgence, SrcNomConfig;
    private JComboBox<ComboItem> SrcListConfig;
    private JButton validerButton, SrcAjoutConfig;
    private JFormattedTextField dateMajConfig;
    private JScrollPane panelScroll;

    private final String databaseSourceFile = "resource/databaseSource.json";
    private final JSONArray infoSource = DataBase.getInfoConnexion(databaseSourceFile);
    private JSONObject object = (JSONObject) (infoSource != null ? infoSource.get(0) : null);
    private ComboItem comboItem;
    private boolean newItem;
    private boolean focusOutName;

    private final Map<String, JCheckBox> checkBoxMap = new HashMap<>();

    public ConfigFile() {
        initCheckBoxMap();
        initValue();

        // Créer un panneau avec le défilement
        panel1.setLayout(new BoxLayout(panel1, BoxLayout.Y_AXIS)); // Ajuster la disposition si nécessaire

        JScrollPane scrollPane = new JScrollPane(panel1, JScrollPane.VERTICAL_SCROLLBAR_ALWAYS, JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);

        validerButton.addActionListener(e -> saveConfiguration());
        SrcAjoutConfig.addActionListener(e -> addNewConfig());

        for (JCheckBox checkBox : checkBoxMap.values()) {
            checkBox.addActionListener(e -> updateConfigData());
        }

        addListeners(SrcServerName, SrcBaseDeDonnees, SrcLogin, SrcMotDePasse, SrcRepertoire, dossierFTPSource, codeAgence);

        SrcRepertoire.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                fileChooser();
            }
        });

        SrcNomConfig.addFocusListener(new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent e) {
                updateConfigData();
                setListConfig(false);
            }
        });

        SrcListConfig.addActionListener(e -> {
            if (!focusOutName) {
                comboItem = (ComboItem) SrcListConfig.getSelectedItem();
                if (!newItem) {
                    getConfigData(Integer.parseInt(comboItem.getValue()));
                }
            }
        });

        addGroupedCheckBoxListener(venteCheckBox, factureDeVenteCheckBox, devisCheckBox, bonDeLivraisonCheckBox);
        addGroupedCheckBoxListener(stockCheckBox, documentDEntréeCheckBox, documentDeSortieCheckBox, documentDeTransfertCheckBox);
        addMutuallyExclusiveCheckBoxListener(EnvoiCheckBox, ReceptionCheckBox);
    }

    private void initCheckBoxMap() {
        checkBoxMap.put("article", SrcArticlesCheckBox);
        checkBoxMap.put("tiers", SrcTiersCheckBox);
        checkBoxMap.put("depot", SrcDepotCheckBox);
        checkBoxMap.put("entete", SrcEnteteCheckBox);
        checkBoxMap.put("ligne", SrcLigneCheckBox);
        checkBoxMap.put("reglement", SrcReglementCheckBox);
        checkBoxMap.put("artstock", SrcArtStockCheckBox);
        checkBoxMap.put("ecriturec", SrcEcritureCCheckBox);
        checkBoxMap.put("ecriturea", SrcEcritureACheckBox);
        checkBoxMap.put("compteg", SrcCompteGeneralCheckBox);
        checkBoxMap.put("taxe", SrcTaxeCheckBox);
        checkBoxMap.put("journaux", SrcJournauxCheckBox);
        checkBoxMap.put("collaborateur", SrcCollaborateurCheckBox);
        checkBoxMap.put("envoi", EnvoiCheckBox);
        checkBoxMap.put("active", ActiveCheckBox);
        checkBoxMap.put("reception", ReceptionCheckBox);
    }

    private void fileChooser() {
        JFileChooser chooser = new JFileChooser(FileSystemView.getFileSystemView().getHomeDirectory());
        if (!SrcRepertoire.getText().equals("")) {
            chooser.setCurrentDirectory(new File(SrcRepertoire.getText()));
        }
        chooser.setDialogTitle("Choisissez un répertoire:");
        chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        int res = chooser.showSaveDialog(null);
        if (res == JFileChooser.APPROVE_OPTION) {
            if (chooser.getSelectedFile().isDirectory()) {
                SrcRepertoire.setText(chooser.getSelectedFile().toString());
            }
        }
    }

    private void initValue() {
        newItem = false;
        setListConfig(false);
        getConfigData(0);
        comboItem = SrcListConfig.getItemAt(0);
    }

    private void setListConfig(boolean ajout) {
        int selected = (SrcListConfig.getSelectedIndex() == -1) ? 0 : SrcListConfig.getSelectedIndex();
        if (ajout && infoSource != null) {
            selected = infoSource.size() - 1;
        }
        SrcListConfig.validate();
        if (infoSource != null) {
            for (int i = 0; i < infoSource.size(); i++) {
                JSONObject object = (JSONObject) infoSource.get(i);
                String nomConfig = (String) object.get("nomconfig");
                ComboItem comboItemAdd = new ComboItem(nomConfig, String.valueOf(i));
                if (i == selected) comboItem = comboItemAdd;
                SrcListConfig.addItem(comboItemAdd);
            }
        }
        SrcListConfig.setSelectedIndex(selected);
    }

    private void addNewConfig() {
        if (infoSource != null) {
            newItem = true;
            infoSource.add(initJson());
            addItemToList();
            setListConfig(true);
            getConfigData(infoSource.size() - 1);
            newItem = false;
        }
    }

    private void addItemToList() {
        if (infoSource != null) {
            SrcListConfig.addItem(new ComboItem("", String.valueOf(infoSource.size() - 1)));
        }
    }

    private JSONObject initJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("servername", "");
        jsonObject.put("nomconfig", "");
        jsonObject.put("database", "");
        jsonObject.put("username", "");
        jsonObject.put("password", "");
        jsonObject.put("path", "");
        jsonObject.put("folderftp", "");
        jsonObject.put("codeagence", "");
        jsonObject.put("article", "0");
        jsonObject.put("tiers", "0");
        jsonObject.put("depot", "0");
        jsonObject.put("entete", "0");
        jsonObject.put("ligne", "0");
        jsonObject.put("reglement", "0");
        jsonObject.put("artstock", "0");
        jsonObject.put("ecriturec", "0");
        jsonObject.put("ecriturea", "0");
        jsonObject.put("compteg", "0");
        jsonObject.put("taxe", "0");
        jsonObject.put("journaux", "0");
        jsonObject.put("collaborateur", "0");
        jsonObject.put("envoi", "0");
        jsonObject.put("active", "1");
        jsonObject.put("reception", "0");
        JSONObject typeDocument = new JSONObject();
        typeDocument.put("facturedevente", "0");
        typeDocument.put("devis", "0");
        typeDocument.put("bondelivraison", "0");
        typeDocument.put("facturedachat", "0");
        typeDocument.put("entree", "0");
        typeDocument.put("sortie", "0");
        typeDocument.put("transfert", "0");
        typeDocument.put("documentinterne1", "0");
        typeDocument.put("documentinterne2", "0");
        jsonObject.put("typedocument", typeDocument);
        return jsonObject;
    }

    private JSONObject updateJson(JSONObject jsonObject) {
        jsonObject.put("servername", SrcServerName.getText());
        jsonObject.put("nomconfig", SrcNomConfig.getText());
        jsonObject.put("database", SrcBaseDeDonnees.getText());
        jsonObject.put("username", SrcLogin.getText());
        jsonObject.put("password", SrcMotDePasse.getText());
        jsonObject.put("path", SrcRepertoire.getText());
        jsonObject.put("folderftp", dossierFTPSource.getText());
        jsonObject.put("codeagence", codeAgence.getText());
        jsonObject.put("article", checkBoxMap.get("article").isSelected() ? "1" : "0");
        jsonObject.put("tiers", checkBoxMap.get("tiers").isSelected() ? "1" : "0");
        jsonObject.put("depot", checkBoxMap.get("depot").isSelected() ? "1" : "0");
        jsonObject.put("entete", checkBoxMap.get("entete").isSelected() ? "1" : "0");
        jsonObject.put("ligne", checkBoxMap.get("ligne").isSelected() ? "1" : "0");
        jsonObject.put("reglement", checkBoxMap.get("reglement").isSelected() ? "1" : "0");
        jsonObject.put("artstock", checkBoxMap.get("artstock").isSelected() ? "1" : "0");
        jsonObject.put("ecriturec", checkBoxMap.get("ecriturec").isSelected() ? "1" : "0");
        jsonObject.put("ecriturea", checkBoxMap.get("ecriturea").isSelected() ? "1" : "0");
        jsonObject.put("compteg", checkBoxMap.get("compteg").isSelected() ? "1" : "0");
        jsonObject.put("taxe", checkBoxMap.get("taxe").isSelected() ? "1" : "0");
        jsonObject.put("journaux", checkBoxMap.get("journaux").isSelected() ? "1" : "0");
        jsonObject.put("collaborateur", checkBoxMap.get("collaborateur").isSelected() ? "1" : "0");
        jsonObject.put("envoi", checkBoxMap.get("envoi").isSelected() ? "1" : "0");
        jsonObject.put("active", checkBoxMap.get("active").isSelected() ? "1" : "0");
        jsonObject.put("reception", checkBoxMap.get("reception").isSelected() ? "1" : "0");

        JSONObject typeDocument = (JSONObject) jsonObject.get("typedocument");
        typeDocument.put("facturedevente", factureDeVenteCheckBox.isSelected() ? "1" : "0");
        typeDocument.put("devis", devisCheckBox.isSelected() ? "1" : "0");
        typeDocument.put("bondelivraison", bonDeLivraisonCheckBox.isSelected() ? "1" : "0");
        typeDocument.put("facturedachat", factureDAchatCheckBox.isSelected() ? "1" : "0");
        typeDocument.put("entree", documentDEntréeCheckBox.isSelected() ? "1" : "0");
        typeDocument.put("sortie", documentDeSortieCheckBox.isSelected() ? "1" : "0");
        typeDocument.put("transfert", documentDeTransfertCheckBox.isSelected() ? "1" : "0");
        typeDocument.put("documentinterne1", documentInterne1CheckBox.isSelected() ? "1" : "0");
        typeDocument.put("documentinterne2", documentInterne2CheckBox.isSelected() ? "1" : "0");
        return jsonObject;
    }

    private void getConfigData(int index) {
        object = (JSONObject) infoSource.get(index);
        SrcServerName.setText((String) object.get("servername"));
        SrcNomConfig.setText((String) object.get("nomconfig"));
        SrcBaseDeDonnees.setText((String) object.get("database"));
        SrcLogin.setText((String) object.get("username"));
        SrcMotDePasse.setText((String) object.get("password"));
        SrcRepertoire.setText((String) object.get("path"));
        dossierFTPSource.setText((String) object.get("folderftp"));
        codeAgence.setText((String) object.get("codeagence"));

        updateCheckBoxSelection("article", "tiers", "depot", "entete", "ligne", "reglement", "artstock",
                "ecriturec", "ecriturea", "compteg", "taxe", "journaux", "collaborateur", "envoi", "active", "reception");

        JSONObject typeDocument = (JSONObject) object.get("typedocument");
        factureDeVenteCheckBox.setSelected("1".equals(typeDocument.get("facturedevente")));
        devisCheckBox.setSelected("1".equals(typeDocument.get("devis")));
        bonDeLivraisonCheckBox.setSelected("1".equals(typeDocument.get("bondelivraison")));
        factureDAchatCheckBox.setSelected("1".equals(typeDocument.get("facturedachat")));
        documentDEntréeCheckBox.setSelected("1".equals(typeDocument.get("entree")));
        documentDeSortieCheckBox.setSelected("1".equals(typeDocument.get("sortie")));
        documentDeTransfertCheckBox.setSelected("1".equals(typeDocument.get("transfert")));
        documentInterne1CheckBox.setSelected("1".equals(typeDocument.get("documentinterne1")));
        documentInterne2CheckBox.setSelected("1".equals(typeDocument.get("documentinterne2")));

        // Update dateMajConfig
        if (object.containsKey("datemaj")) {
            String dateMajStr = (String) object.get("datemaj");
            try {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                Date date = dateFormat.parse(dateMajStr);
                dateMajConfig.setText(new SimpleDateFormat("dd/MM/yyyy").format(date));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        } else {
            dateMajConfig.setText(DateTimeFormatter.ofPattern("dd/MM/yyyy").format(LocalDate.now()));
        }
    }

    private void updateCheckBoxSelection(String... keys) {
        for (String key : keys) {
            JCheckBox checkBox = checkBoxMap.get(key);
            checkBox.setSelected("1".equals(object.get(key)));
        }
    }

    private void saveConfiguration() {
        if (comboItem != null) {
            updateConfigData();
            // Mettre à jour la date de modification
            String dateAffichee = dateMajConfig.getText();
            DateTimeFormatter formatterAffichee = DateTimeFormatter.ofPattern("dd/MM/yyyy");
            LocalDate date = LocalDate.parse(dateAffichee, formatterAffichee);
            DateTimeFormatter formatterJSON = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            String datePourJSON = date.format(formatterJSON);
            object.put("datemaj", datePourJSON);

            DataBase.saveInfoConnexion(infoSource, databaseSourceFile);
        }
    }

    private void addListeners(JTextField... textFields) {
        for (JTextField textField : textFields) {
            textField.addFocusListener(new FocusAdapter() {
                @Override
                public void focusLost(FocusEvent e) {
                    updateConfigData();
                }
            });
        }
    }

    private void addGroupedCheckBoxListener(JCheckBox masterCheckBox, JCheckBox... groupedCheckBoxes) {
        masterCheckBox.addItemListener(e -> {
            boolean selected = e.getStateChange() == ItemEvent.SELECTED;
            for (JCheckBox checkBox : groupedCheckBoxes) {
                checkBox.setSelected(selected);
            }
            updateConfigData();
        });
    }

    private void addMutuallyExclusiveCheckBoxListener(JCheckBox checkBox1, JCheckBox checkBox2) {
        ItemListener listener = e -> {
            if (e.getSource() == checkBox1 && checkBox1.isSelected()) {
                checkBox2.setSelected(false);
            } else if (e.getSource() == checkBox2 && checkBox2.isSelected()) {
                checkBox1.setSelected(false);
            }
            updateConfigData();
        };
        checkBox1.addItemListener(listener);
        checkBox2.addItemListener(listener);
    }


    private void updateConfigData() {
        if (comboItem != null && object != null) {
            object = updateJson(object);
            infoSource.set(Integer.parseInt(comboItem.getValue()), object);
        }
    }

    public static void main(String[] args) {
        JFrame frame = new JFrame("ConfigFile");
        ConfigFile configFile = new ConfigFile();

        // Ajouter le panneau défilant
        JScrollPane scrollPane = new JScrollPane(configFile.panel1);
        frame.setContentPane(scrollPane);

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

    private void createUIComponents() {
        // TODO: place custom component creation code here
    }
    public class DataBase {
        public static JSONArray getInfoConnexion(String databaseSourceFile) {
            JSONParser jsonParser = new JSONParser();

            try (FileReader reader = new FileReader(databaseSourceFile)) {
                // Lire le fichier JSON et le parser en JSONArray
                Object obj = jsonParser.parse(reader);
                return (JSONArray) obj;
            } catch (IOException e) {
                e.printStackTrace();
                return new JSONArray(); // Retourne un tableau vide en cas d'erreur
            } catch (org.json.simple.parser.ParseException e) {
                throw new RuntimeException(e);
            }
        }
        public static void saveInfoConnexion(JSONArray infoSource, String databaseSourceFile) {
            try (FileWriter file = new FileWriter(databaseSourceFile)) {
                file.write(infoSource.toJSONString());
                file.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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

        public String getValue()
        {
            return value;
        }
    }

}
