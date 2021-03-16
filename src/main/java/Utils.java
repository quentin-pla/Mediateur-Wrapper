/**
 * Fonctions utilitaires
 */
public class Utils {
    /**
     * Lever une exception
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
}
