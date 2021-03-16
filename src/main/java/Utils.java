import static java.lang.Math.*;

/**
 * Fonctions utilitaires
 */
public class Utils {
    /**
     * Lever une exception
     *
     * @param error erreur
     */
    public static void throwException(String error) {
        try {
            throw new Exception(error);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Calculer la distance approximative en mètres entre deux coordonnées GPS
     * (source : https://www.movable-type.co.uk/scripts/latlong.html)
     *
     * @param lat1 latitude 1
     * @param lon1 longitude 1
     * @param lat2 latitude 2
     * @param lon2 longitude 2
     * @return distance (m)
     */
    public static double distance(double lat1, double lon1, double lat2, double lon2) {
        // Conversion en radians
        lat1 = toRadians(lat1);
        lon1 = toRadians(lon1);
        lat2 = toRadians(lat2);
        lon2 = toRadians(lon2);

        // Rayon approximatif de la terre
        double R = 6373.0;

        double x = (lon2 - lon1) * cos((lat1 + lat2) / 2);
        double y = lat2 - lat1;

        // Distance en mètres
        return round(sqrt(x * x + y * y) * R * 1000);
    }
}
