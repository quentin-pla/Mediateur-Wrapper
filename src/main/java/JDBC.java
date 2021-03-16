import org.sqlite.Function;

import java.sql.*;
import java.util.*;

/**
 * Utilitaire fonctions JDBC
 */
public class JDBC {
    /**
     * Liste des connexions mises en cache
     */
    private final Map<String, Connection> connections;

    /**
     * Instance unique
     */
    private static JDBC instance;

    /**
     * Constructeur
     */
    private JDBC() {
        connections = new HashMap<>();
    }

    /**
     * Récupérer l'instance unique
     *
     * @return instance
     */
    private static JDBC getInstance() {
        if (instance == null) instance = new JDBC();
        return instance;
    }

    /**
     * Exécuter une requête avec connexion
     *
     * @param db_name nom de la base de données
     * @param query   requête
     * @return résultat
     */
    public static List<List<String>> queryDB(String db_name, String query) {
        List<List<String>> result = queryDB(createStatement(db_name), query);
        closeConnection(db_name);
        return result;
    }

    /**
     * Exécuter une requête SQL sans connexion
     *
     * @param statement communication
     * @param query     requête
     * @return résultat
     */
    public static List<List<String>> queryDB(Statement statement, String query) {
        try {
            ResultSet rs = statement.executeQuery(query);
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            final int columnCount = resultSetMetaData.getColumnCount();
            List<List<String>> result = new ArrayList<>();
            // Ajout de la liste des attributs en tête de liste
            List<String> attributes = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++)
                attributes.add(resultSetMetaData.getColumnName(i));
            result.add(attributes);
            // Parcours des tuples
            while (rs.next()) {
                List<String> tuple = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++)
                    tuple.add(rs.getObject(i) == null ? "" : rs.getObject(i).toString());
                result.add(tuple);
            }
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Exécuter une mise à jour avec connexion
     *
     * @param db_name nom de la base de données
     * @param query   requête de mise à jour
     */
    public static void updateDB(String db_name, String query) {
        updateDB(createStatement(db_name), query);
        closeConnection(db_name);
    }

    /**
     * Exécuter une mise à jour sans connexion
     *
     * @param statement communication
     * @param query     requête de mise à jour
     */
    public static void updateDB(Statement statement, String query) {
        try {
            statement.executeUpdate(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Créer une fonction sur la base de données
     *
     * @param db_name       nom de la base de données
     * @param function_name nom de la fonction
     * @param function      fonction
     */
    public static void createSQLFunction(String db_name, String function_name, Function function) {
        try {
            Function.create(getDBConnection(db_name), function_name, function);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Créer une communication dans une base de données
     *
     * @param db_name nom de la base de données
     * @return communication
     */
    public static Statement createStatement(String db_name) {
        Statement statement = null;
        try {
            statement = getDBConnection(db_name).createStatement();
            if (statement == null)
                Utils.throwException("Communication avec la base de données " + db_name + " impossible.");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return statement;
    }

    /**
     * Récupérer la connexion à une base de données
     *
     * @param db_name nom de la base de données
     * @return connexion
     */
    public static Connection getDBConnection(String db_name) {
        // Récupération des connexions mises en cache
        Map<String, Connection> cached_connections = getInstance().connections;
        Connection connection = null;
        if (cached_connections.containsKey(db_name)) {
            connection = cached_connections.get(db_name);
        } else {
            try {
                connection = DriverManager.getConnection("jdbc:sqlite:" + (db_name == null ? "" : db_name + ".db"));
                if (connection == null)
                    Utils.throwException("Connexion à la base de données " + db_name + " impossible.");
                cached_connections.put(db_name, connection);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    /**
     * Fermer une connexion à une base de données
     *
     * @param db_name nom de la base de données
     */
    public static void closeConnection(String db_name) {
        Connection connection = getDBConnection(db_name);
        try {
            if (connection != null) connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Afficher les résultats d'une requête SQL
     *
     * @param data données
     */
    public static void showQueryResult(List<List<String>> data) {
        int[] column_spaces = new int[data.get(0).size()];
        for (List<String> tuple : data) {
            for (int i = 0; i < tuple.size(); i++) {
                int value_length = tuple.get(i).length();
                if (column_spaces[i] < value_length)
                    column_spaces[i] = value_length;
            }
        }
        for (List<String> tuple : data) {
            for (int i = 0; i < tuple.size(); i++) {
                int remaining_length = Math.abs(tuple.get(i).length() - column_spaces[i]) + 3;
                System.out.print("| " + tuple.get(i) + " ".repeat(remaining_length));
            }
            System.out.println();
        }
    }
}
