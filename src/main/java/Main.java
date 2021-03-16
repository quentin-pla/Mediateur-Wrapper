import org.sqlite.Function;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Main
 */
public class Main {
    public static void main(String[] args) {
        if (args.length > 0 && args.length <= 2) {
            // Vérification du premier argument
            boolean first_arg_valid = true;
            int query_number = 0;
            try {
                query_number = Integer.parseInt(args[0]);
                if (query_number < 1 || query_number > 3)
                    first_arg_valid = false;
            } catch (NumberFormatException e) {
                first_arg_valid = false;
            }
            if (!first_arg_valid)
                Utils.throwException("Premier argument invalide (compris entre 1 - 3).");

            // Vérification de second argument
            boolean debug_mode = false;
            if (args.length == 2) {
                if (args[1].equals("--debug")) debug_mode = true;
                else Utils.throwException("Second argument invalide (\"--debug\" pour activer le mode de débugage).");
            }

            // Médiateur
            Mediator mediator = initializeMediator(debug_mode);

            // Exécution de la requête de test demandée
            mediator.executeSelectRequest(getSampleSQLQueries().get(query_number - 1));
        } else Utils.throwException("Arguments invalides.\n" +
                "Argument n°1 : numéro de la requête de test à exécuter.\n" +
                "Argument n°2 (facultatif) : activer le mode de débugage avec \"--debug\")");
    }

    /**
     * Initialiser le médiateur
     *
     * @param debug_mode mode de débugage
     * @return médiateur SQL
     */
    private static Mediator initializeMediator(boolean debug_mode) {
        Mediator mediator = new Mediator(debug_mode);

        // Intégration des fichiers CSV
        mediator.addDataFromCSV("fontaine", "fontaines.csv", ';');
        mediator.addDataFromCSV("activite", "equipements_activites.csv", ';');
        mediator.addDataFromCSV("parc", "espaces_verts.csv", ';');

        // Création d'une fonction SQL "DISTANCE" pour obtenir la distance approximative entre deux coordonnées GPS
        mediator.createSQLFunction("DISTANCE", new Function() {
            @Override
            protected void xFunc() throws SQLException {
                if (args() == 4) {
                    // Latitude 1, Longitude 1, Latitude 2, Longitude 2
                    result(Utils.distance(value_double(0), value_double(1),
                            value_double(2), value_double(3)));
                } else {
                    Utils.throwException("Nombre d'arguments invalides.");
                }
            }
        });

        return mediator;
    }

    /**
     * Liste des requêtes de test disponibles
     *
     * @return requêtes SQL
     */
    private static List<String> getSampleSQLQueries() {
        List<String> available_queries = new ArrayList<>();

        // 1. Requête permettant de voir les parcs ouverts pendant la canicule
        // avec une fontaine d'eau potable à moins de 200m
        available_queries.add(String.join("\n",
                "SELECT PARC.ID, PARC.NOM, PARC.ARRONDISSEMENT, FONTAINE.ID, FONTAINE.TYPE, FONTAINE.COMMUNE",
                "FROM PARC, FONTAINE",
                "WHERE PARC.TYPE = \"Promenades ouvertes\"",
                "AND PARC.OUVERTURE_CANICULE = \"Oui\"",
                "AND DISTANCE(PARC.LATITUDE, PARC.LONGITUDE, FONTAINE.LATITUDE, FONTAINE.LONGITUDE) <= 200",
                "GROUP BY PARC.ID"));

        // 2. Requête permettant de voir les activités touristiques aux alentours d'un parc (moins de 500m)
        available_queries.add(String.join("\n",
                "SELECT ACTIVITE.ID, ACTIVITE.NOM, ACTIVITE.ARRONDISSEMENT, PARC.ID, PARC.NOM, PARC.ARRONDISSEMENT",
                "FROM ACTIVITE, PARC",
                "WHERE PARC.TYPE = \"Promenades ouvertes\"",
                "AND PARC.OUVERTURE_CANICULE = \"Oui\"",
                "AND DISTANCE(ACTIVITE.LATITUDE, ACTIVITE.LONGITUDE, PARC.LATITUDE, PARC.LONGITUDE) <= 500",
                "GROUP BY ACTIVITE.ID"));

        // 3. Requête permettant de voir les fontaines potables aux alentours (200m) de la position GPS d'un utilisateur
        // Exemple avec un utilisateur situé à l'adresse suivante : Esplanade Jacques Chaban-Delmas, 75007 Paris
        // Avec la position GPS suivante : 48.852543, 2.312251
        available_queries.add(String.join("\n",
                "SELECT ID, TYPE, VOIE, COMMUNE",
                "FROM FONTAINE",
                "WHERE DISTANCE(48.852543, 2.312251, LATITUDE, LONGITUDE) <= 200"));

        return available_queries;
    }
}
