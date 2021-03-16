import org.sqlite.Function;

import java.sql.Statement;
import java.util.*;

/**
 * Médiateur SQL
 */
public class Mediator {
    /**
     * Mode de débugage
     */
    private final boolean debug_mode;

    /**
     * Liste des sources
     */
    private final ArrayList<Wrapper> sources;

    /**
     * Liste des attributs des sources
     */
    private final Map<Wrapper, List<String>> sources_attributes;

    /**
     * Table liée à chauqe source
     */
    private final Map<Wrapper, String> sources_tables;

    /**
     * Liaison table - attributs
     */
    private final Map<String, LinkedHashSet<String>> tables_attributes;

    /**
     * Constructeur
     */
    public Mediator(boolean _debug_mode) {
        debug_mode = _debug_mode;
        sources = new ArrayList<>();
        sources_tables = new HashMap<>();
        sources_attributes = new HashMap<>();
        tables_attributes = new HashMap<>();
    }

    /**
     * Ajouter des données sources provenant d'un fichier CSV
     * @param file_path nom du fichier
     * @param delimiter délimiteur
     */
    public void addDataFromCSV(String table_name, String file_path, char delimiter) {
        // On vérifie que le fichier n'est pas déjà contenu dans les sources
        for (Wrapper source : sources) {
            if (source.file_path.equals(file_path)) {
                System.err.println("ATTENTION : Source déjà importée.");
                return;
            }
        }
        // Ajout des sources cas échéant
        Wrapper source = new WrapperCSV("S" + sources.size(), table_name.toUpperCase(),
                file_path, delimiter, debug_mode);
        sources.add(source);
        generateSourceCapacities(source);
    }

    /**
     * Générer les capacités d'une source
     * @param source source
     */
    private void generateSourceCapacities(Wrapper source) {
        sources_tables.put(source, source.table_name);
        sources_attributes.put(source, source.attributes);
        if (!tables_attributes.containsKey(source.table_name))
            tables_attributes.put(source.table_name, new LinkedHashSet<>());
        tables_attributes.get(source.table_name).addAll(source.attributes);
    }

    /**
     * Exécuter une requête SQL
     */
    public void executeSelectRequest(String sql_request) {
        sql_request = sql_request.toUpperCase();
        if (debug_mode) System.out.println("> Préparation de la requête : " + sql_request);
        String views_request = decomposeRequest(analyzeSQLRequest(sql_request));
        if (debug_mode) System.out.println("> Requête décomposée : \n\n" + views_request);
        Statement statement = JDBC.createStatement(null);
        System.out.println("> Mise à jour de la base de données temporaire...");
        JDBC.updateDB(statement, views_request);
        System.out.println("> Exécution de la requête...");
        Map<String, List<String>> result = JDBC.queryDB(statement, sql_request);
        System.out.println("> Résultats : \n");
        System.out.println(result);
        JDBC.closeConnection(null);
    }

    /**
     * Analyser une requête SQL
     * @param sql_request requête SQL
     */
    public Map<String, List<String>> analyzeSQLRequest(String sql_request) {
        if (debug_mode) System.out.println("> Analyse de la requête...");
        int select_index = sql_request.indexOf("SELECT ");
        int from_index = sql_request.indexOf(" FROM ");
        int where_index = sql_request.indexOf(" WHERE ");
        if (!sql_request.startsWith("SELECT ")) Utils.throwException("Requête invalide.");
        // S'il n'y a pas le mot clé 'FROM', il s'agit sûrement de l'exécution d'une fonction
        if (from_index == -1) return new HashMap<>();
        // Récupération des tables
        List<String> tables = Arrays.asList(sql_request.substring(from_index + 5,
                where_index > -1 ? where_index : sql_request.length()).split(","));
        for (int i = 0; i < tables.size(); i++)
            tables.set(i, tables.get(i).replaceAll(" ", ""));

        // Récupération des attributs
        List<String> attributes = Arrays.asList(sql_request.substring(select_index + 7, from_index).split(","));
        for (int i = 0; i < attributes.size(); i++) {
            String attribute = attributes.get(i);
            // On ne récupère seulement le nom de l'attribut, les autres paramètres sont ignorés
            if (attribute.contains("(") && attribute.contains(")"))
                attribute = attribute.substring(attribute.indexOf("(") + 1, attribute.indexOf(")"));
            if (attribute.contains("."))
                attribute = attribute.substring(attribute.indexOf(".") + 1);
            if (attribute.contains(" AS "))
                attribute = attribute.substring(0, attribute.indexOf(" AS "));
            attributes.set(i, attribute.replaceAll(" ", ""));
        }

        Map<String, List<String>> result = new HashMap<>();

        // Vérification des tables
        for (String table : tables) {
            if (!tables_attributes.containsKey(table)) Utils.throwException("Requête invalide.");
            result.put(table, new ArrayList<>());
        }

        // Vérification des attributs
        if (attributes.get(0).equals("*")) {
            for (String table : tables)
                result.get(table).add("*");
        } else {
            for (String attribute : attributes) {
                boolean found = false;
                for (Map.Entry<String, LinkedHashSet<String>> table_entry : tables_attributes.entrySet()) {
                    LinkedHashSet<String> table_attributes = table_entry.getValue();
                    if (table_attributes.contains(attribute) && !result.get(table_entry.getKey()).contains(attribute)) {
                        found = true;
                        result.get(table_entry.getKey()).add(attribute);
                        break;
                    }
                }
                if (!found) Utils.throwException("Requête invalide.");
            }
        }

        return result;
    }

    /**
     * Décomposer une requête SQL
     * @param query_elements éléments obtenus à partir de la requête SQL
     */
    private String decomposeRequest(Map<String, List<String>> query_elements) {
        if (debug_mode) System.out.println("> Décomposition de la requête...");
        // Initialisation des requêtes SQL pour la création des vues
        Map<String, StringBuilder> tables_views = new HashMap<>();
        for (String table : query_elements.keySet())
            tables_views.put(table, new StringBuilder("CREATE TEMP VIEW " + table + " AS\n"));

        // Récupération des wrappers utiles pour la requête
        List<Wrapper> useful_wrappers = new ArrayList<>();
        for (Wrapper wrapper : sources)
            if (query_elements.containsKey(wrapper.table_name))
                useful_wrappers.add(wrapper);

        // Remplissage des requêtes avec chaque wrapper et attributs utiles
        for (Wrapper wrapper : useful_wrappers) {
            StringBuilder query = tables_views.get(wrapper.table_name);
            query.append("  SELECT ");
            if (query_elements.get(wrapper.table_name).get(0).equals("*")) {
                query.append("*");
            } else {
                for (String attribute : query_elements.get(wrapper.table_name)) {
                    if (wrapper.attributes.contains(attribute)) query.append(attribute + ",");
                    else query.append("NULL,");
                }
                query.setLength(query.length() - 1);
            }
            query.append("\n  FROM S" + sources.indexOf(wrapper) + "." + wrapper.table_name + "\nUNION\n");
        }

        // Requête finale
        StringBuilder views_query = new StringBuilder();

        // Attachement des bases de données utilisées par les vues afin de parcourir les tables utilisées
        for (Wrapper wrapper : useful_wrappers) {
            String db_name = "S" + sources.indexOf(wrapper);
            views_query.append("ATTACH '" + db_name + ".db' AS " + db_name + ";\n");
        }
        views_query.append("\n");

        // Regroupement de toutes les requêtes de vues
        for (StringBuilder query : tables_views.values()) {
            query.setLength(query.length() - 7);
            views_query.append(query + ";\n");
        }

        return views_query.toString();
    }

    /**
     * Créer une fonction SQL
     * @param name nom de la fonction
     * @param function fonction
     */
    public void createSQLFunction(String name, Function function) {
        JDBC.createSQLFunction(null, name, function);
    }
}
