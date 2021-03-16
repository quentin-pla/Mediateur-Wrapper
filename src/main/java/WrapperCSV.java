import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;

import java.io.*;
import java.util.Arrays;

/**
 * Wrapper CSV vers SQL
 */
public class WrapperCSV extends Wrapper {
    /**
     * Constructeur
     *
     * @param db_name    nom de la base de données
     * @param table_name nom de la table
     * @param file_path  chemin vers le fichier
     * @param delimiter  délimiteur
     * @param debug_mode mode de débugage
     */
    public WrapperCSV(String db_name, String table_name, String file_path, char delimiter, boolean debug_mode) {
        super(db_name, table_name, file_path, debug_mode);
        readFile(file_path, delimiter + "");
        String query = convertDataToSQL();
        if (debug_mode) System.out.println("> Mise à jour de la BDD " + db_name + "...");
        JDBC.updateDB(db_name, query);
        if (debug_mode) System.out.println("> Intégration terminée.\n");
    }

    @Override
    protected void readFile(String file_path, String... args) {
        // Vérification des arguments
        if (args.length != 1 || args[0].length() > 1) Utils.throwException("Arguments invalides.");
        if (debug_mode) System.out.println("> Lecture du fichier : " + file_path + "...");
        CSVParser parser = new CSVParserBuilder().withSeparator(args[0].charAt(0)).build();
        Reader reader = null;
        if (file_path.lastIndexOf("/") > 0) {
            try {
                reader = new FileReader(file_path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            InputStream input = WrapperCSV.class.getClassLoader().getResourceAsStream(file_path);
            if (input != null) reader = new InputStreamReader(input);
            else Utils.throwException("Fichier CSV introuvable.");
        }
        if (reader != null) {
            CSVReader csv_reader = new CSVReaderBuilder(reader).withCSVParser(parser).build();
            try {
                values = csv_reader.readAll();
            } catch (IOException | CsvException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected String convertDataToSQL() {
        if (debug_mode) System.out.println("> Création de la requête SQL...");
        StringBuilder sb = new StringBuilder();
        // Suppression et création table
        sb.append("DROP TABLE IF EXISTS \"" + table_name + "\";\n");
        sb.append("CREATE TABLE \"" + table_name + "\" (\n");

        // Récupération des attributs
        attributes = Arrays.asList(values.get(0));
        values.remove(0);
        sb.append(attributes.get(0) + " " + getAttributeType(attributes.get(0)) + " PRIMARY KEY");
        for (int i = 1; i < attributes.size(); i++)
            sb.append(",\n" + attributes.get(i) + " " + getAttributeType(attributes.get(i)));
        sb.append(");\n");

        // Préparation requête insertion
        StringBuilder begin_insert_query = new StringBuilder("INSERT INTO \"" + table_name + "\" (");
        for (String attribute : attributes)
            begin_insert_query.append(attribute + ",");
        begin_insert_query.setLength(begin_insert_query.length() - 1);
        begin_insert_query.append(") VALUES (");

        // Création des requêtes d'insertion
        for (String[] tuple : values) {
            sb.append(begin_insert_query);
            for (String value : tuple)
                sb.append(value.isEmpty() ? "NULL," : "'" + value.replaceAll("'", "''") + "',");
            sb.setLength(sb.length() - 1);
            sb.append(");\n");
        }
        return sb.toString();
    }
}
