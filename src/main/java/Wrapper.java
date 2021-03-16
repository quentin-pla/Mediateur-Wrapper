import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper SQL abstrait
 */
public abstract class Wrapper {
    /**
     * Mode de débugage
     */
    protected final boolean debug_mode;

    /**
     * Chemin vers le fichier
     */
    protected String file_path;

    /**
     * Nom de la base de données
     */
    protected String db_name;

    /**
     * Nom de la table
     */
    protected String table_name;

    /**
     * Liste des attributs
     */
    protected List<String> attributes;

    /**
     * Données du fichier
     */
    protected List<String[]> values;

    /**
     * Constructeur
     *
     * @param _db_name    nom de la base de données
     * @param _table_name nom de la table
     * @param _file_path  chemin du fichier
     * @param _debug_mode mode de débugage
     */
    protected Wrapper(String _db_name, String _table_name, String _file_path, boolean _debug_mode) {
        debug_mode = _debug_mode;
        file_path = _file_path;
        db_name = _db_name;
        table_name = _table_name;
        attributes = new ArrayList<>();
        values = new ArrayList<>();
    }

    /**
     * Lire un fichier
     *
     * @param file_path chemin du fichier
     * @param args      arguments
     */
    protected abstract void readFile(String file_path, String... args);

    /**
     * Convertir les données d'un fichier en une requête SQL
     * permettant de créer la table associée aux données et de la remplir
     *
     * @return requête SQL
     */
    protected abstract String convertDataToSQL();

    /**
     * Obtenir la liste des valeurs d'un attribut
     *
     * @param attribute attribut
     * @return liste des valeurs
     */
    protected List<String> getAttributeData(String attribute) {
        int attribute_index = attributes.indexOf(attribute);
        if (attribute_index > -1) {
            List<String> data = new ArrayList<>();
            for (String[] value : values) data.add(value[attribute_index]);
            return data;
        }
        return null;
    }

    /**
     * Obtenir le type d'un attribut
     *
     * @param attribute attribut
     * @return type de l'attribut
     */
    protected String getAttributeType(String attribute) {
        List<String> attribute_data = getAttributeData(attribute);
        String attribute_type = "TEXT";
        boolean onlyFloating = true;
        boolean onlyInteger = true;

        for (String string : attribute_data) {
            if (!onlyFloating && !onlyInteger)
                break;
            if (onlyFloating && !string.matches("[-+]?[0-9]*\\.?[0-9]+"))
                onlyFloating = false;
            if (!string.matches("[0-9]+"))
                onlyInteger = false;
        }

        if (onlyFloating) attribute_type = "REAL";
        else if (onlyInteger) attribute_type = "INTEGER";

        if (!attribute_data.contains("")) attribute_type += " NOT NULL";

        return attribute_type;
    }
}
