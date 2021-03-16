import org.sqlite.Function;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Main
 */
public class Main {
    public static void main(String[] args) {
        Mediator mediator = new Mediator(true);
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

        // Liste des requêtes diponibles
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
                "SELECT ID, TYPE, COMMUNE",
                "FROM FONTAINE",
                "WHERE DISTANCE(48.852543, 2.312251, LATITUDE, LONGITUDE) <= 200"));

        // Exécution de la requête
        mediator.executeSelectRequest(available_queries.get(2));
    }
}
