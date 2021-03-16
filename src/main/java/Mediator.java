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
     *
     * @param _debug_mode mode de débugage
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
     *
     * @param table_name nom de la table
     * @param file_path  nom du fichier
     * @param delimiter  délimiteur
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
     *
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
     *
     * @param sql_request requête SQL
     */
    public void executeSelectRequest(String sql_request) {
        String views_request = decomposeRequest(analyzeSQLRequest(sql_request));
        if (debug_mode) System.out.println("> Requête décomposée et optimisée : \n\n" + views_request);
        Statement statement = JDBC.createStatement(null);
        if (debug_mode) System.out.println("> Mise à jour de la base de données temporaire...");
        JDBC.updateDB(statement, views_request);
        if (debug_mode) System.out.println("> Exécution de la requête : \n\n" + sql_request + "\n");
        List<List<String>> result = JDBC.queryDB(statement, sql_request);
        if (debug_mode) System.out.println("> Résultats : \n");
        JDBC.showQueryResult(result);
        JDBC.closeConnection(null);
    }

    /**
     * Récupérer la liste des tables utiles contenues dans une requête SQL
     *
     * @param query requête
     * @return liste des tables utiles
     */
    private List<String> retrieveSQLQueryTables(String query) {
        int from_index = query.indexOf("FROM");
        int where_index = query.indexOf("WHERE");
        List<String> tables = Arrays.asList(query.substring(from_index + 5,
                where_index > -1 ? where_index - 1 : query.length()).split(","));
        for (int i = 0; i < tables.size(); i++)
            tables.set(i, tables.get(i).replaceAll(" ", ""));
        return tables;
    }

    /**
     * Récupérer la liste des attributs utiles contenus dans une requête SQL
     *
     * @param query requête
     * @return liste des attributs utiles
     */
    private List<String> retrieveSQLQueryAttributes(String query) {
        int select_index = query.indexOf("SELECT");
        int from_index = query.indexOf("FROM");
        int where_index = query.indexOf("WHERE");
        int group_index = query.indexOf("GROUP BY");
        List<String> attributes = new ArrayList<>();
        attributes.addAll(Arrays.asList(query.substring(select_index + 7, from_index - 1).split(",")));
        attributes.addAll(Arrays.asList(query.substring(where_index + 6,
                group_index > -1 ? group_index - 1 : query.length() - 1).split("AND")));
        return attributes;
    }

    /**
     * Séparer les attributs groupés pour récupérer les sous-attributs
     *
     * @param attributes attributs
     */
    private void splitGroupedAttributes(List<String> attributes) {
        Map<String, List<String>> grouped_attributes = new HashMap<>();
        for (String attribute : attributes) {
            int lpar_pos = attribute.indexOf("(");
            int comma_pos = attribute.indexOf(",");
            int rpar_pos = attribute.indexOf(")");
            if (lpar_pos > -1 && comma_pos > -1 && rpar_pos > -1) {
                List<String> sub_attributes = Arrays.asList(attribute.substring(lpar_pos + 1, rpar_pos).split(","));
                grouped_attributes.put(attribute, sub_attributes);
            }
        }
        // Remplacement des attributs groupés par leur sous-attributs
        for (Map.Entry<String, List<String>> entry : grouped_attributes.entrySet()) {
            attributes.remove(entry.getKey());
            attributes.addAll(entry.getValue());
        }
    }

    /**
     * Reformater une liste d'attribut
     *
     * @param attributes liste d'attributs
     */
    private void reformatAttributes(List<String> attributes) {
        for (int i = 0; i < attributes.size(); i++) {
            String attribute = attributes.get(i).toUpperCase();
            if (attribute.contains("("))
                attribute = attribute.substring(attribute.indexOf("(") + 1);
            if (attribute.contains(")"))
                attribute = attribute.substring(0, attribute.indexOf(")"));
            if (attribute.contains(" AS "))
                attribute = attribute.substring(0, attribute.indexOf(" AS "));
            if (attribute.contains("="))
                attribute = attribute.substring(0, attribute.indexOf("="));
            attributes.set(i, attribute.replaceAll(" ", ""));
        }
    }

    /**
     * Vérifier que des attributs sont bien existants
     *
     * @param tables tables utiles
     * @param attributes attributs
     * @return attributs filtrés
     */
    private Map<String, List<String>> verifyAndFilterAttributes(List<String> tables, List<String> attributes) {
        Map<String, List<String>> filtered = new HashMap<>();

        // Ajout des tables dans les filtres
        for (String table : tables)
            if (!tables_attributes.containsKey(table)) Utils.throwException("Requête invalide.");
            else filtered.put(table, new ArrayList<>());

        // Filtrage des attributs
        if (attributes.get(0).equals("*")) {
            for (String table : tables)
                filtered.get(table).add("*");
        } else {
            for (String attribute : attributes) {
                boolean found = false;
                // Si c'est une chaine de caractère on passe au suivant
                if (attribute.startsWith("\"") && attribute.endsWith("\"") ||
                        attribute.startsWith("'") && attribute.endsWith("'")) {
                    continue;
                } else if (attribute.contains(".")) {
                    // On vérifie que ce n'est pas un nombre
                    try {
                        Double.parseDouble(attribute);
                        continue;
                    } catch (NumberFormatException e) {
                        // S'il contient un point, on a accès à la table dans laquelle il est contenu
                        int dot_pos = attribute.indexOf(".");
                        String possible_table = attribute.substring(0, dot_pos);
                        attribute = attribute.substring(dot_pos + 1);
                        if (tables_attributes.get(possible_table).contains(attribute)) {
                            found = true;
                            filtered.get(possible_table).add(attribute);
                        }
                    }
                } else {
                    // Sinon on recherche une table à laquelle il pourrait appartenir
                    for (String table : tables) {
                        LinkedHashSet<String> table_attributes = tables_attributes.get(table);
                        if (table_attributes.contains(attribute) && !filtered.get(table).contains(attribute)) {
                            found = true;
                            filtered.get(table).add(attribute);
                            break;
                        }
                    }
                }
                if (!found) Utils.throwException("Requête invalide.");
            }
        }

        return filtered;
    }

    /**
     * Analyser une requête SQL
     *
     * @param sql_request requête SQL
     * @return éléments soutirés de la requête SQL
     */
    private Map<String, List<String>> analyzeSQLRequest(String sql_request) {
        if (debug_mode) System.out.println("> Analyse de la requête...");
        if (!sql_request.startsWith("SELECT") || !sql_request.contains("FROM"))
            Utils.throwException("Requête invalide.");

        // Récupération des tables et attributs depuis la requête
        List<String> tables = retrieveSQLQueryTables(sql_request);
        List<String> attributes = retrieveSQLQueryAttributes(sql_request);

        // Séparation des attributs groupés (par exemple ceux dans une fonction)
        splitGroupedAttributes(attributes);

        // Reformatage des attributs
        reformatAttributes(attributes);

        // Vérification et filtrage des attributs par table
        return verifyAndFilterAttributes(tables, attributes);
    }

    /**
     * Décomposer une requête SQL
     *
     * @param query_elements éléments obtenus à partir de la requête SQL
     * @return requête décomposée et optimisée
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
            views_query.append(query + ";\n\n");
        }
        views_query.setLength(views_query.length() - 1);

        return views_query.toString();
    }

    /**
     * Créer une fonction SQL
     *
     * @param name     nom de la fonction
     * @param function fonction
     */
    public void createSQLFunction(String name, Function function) {
        JDBC.createSQLFunction(null, name, function);
    }
}
