import org.sqlite.Function;

/**
 * Main
 */
public class Main {
    public static void main(String[] args) {
        Mediator mediator = new Mediator(true);
//        mediator.addDataFromCSV("fontaine","fontaines.csv",';');
//        mediator.addDataFromCSV("activite","equipements_activites.csv",';');
//        mediator.addDataFromCSV("parc","espaces_verts.csv",';');
//
        mediator.createSQLFunction("A", new Function() {
            @Override
            protected void xFunc() {
                System.out.println("myFunc called!");
            }
        });

//        mediator.executeSelectRequest("SELECT COUNT(PARC.ID) AS IDP FROM PARC");
        mediator.executeSelectRequest("SELECT A();");





//        System.out.println("Création d'une vue de test...");
//
//        // Vue permettant de voir les espaces verts frais ouverts pendant la canicule avec une fontaine d'eau potable à proximité
//        StringBuilder view = new StringBuilder("DROP VIEW IF EXISTS espaces_verts_fontaines;");
//        view.append("CREATE VIEW espaces_verts_fontaines AS");
//
//        view.append(" SELECT id_espace_vert, nom, id_fontaine");
//
//        view.append(" FROM (");
//        view.append("   SELECT identifiant AS id_espace_vert, nom, type, statut_ouverture,");
//        view.append("   canicule_ouverture, substr(geo_point_2d,0,instr(geo_point_2d,',')) AS ev_pos_lat,");
//        view.append("   substr(geo_point_2d,instr(geo_point_2d,',') + 1) AS ev_pos_lng");
//        view.append("   FROM [ilots-de-fraicheur-espaces-verts-frais]");
//        view.append("   WHERE type = \"Promenades ouvertes\"");
//        view.append("   AND statut_ouverture = \"Ouvert\"");
//        view.append("   AND canicule_ouverture = \"Oui\"");
//        view.append("),(");
//        view.append("   SELECT gid AS id_fontaine, substr(geo_point_2d,0,instr(geo_point_2d,',')) AS f_pos_lat,");
//        view.append("   substr(geo_point_2d,instr(geo_point_2d,',') + 1) AS f_pos_lng");
//        view.append("   FROM [fontaines-a-boire]");
//        view.append(")");
//        view.append(" WHERE power(ev_pos_lat - f_pos_lat, 2) + power(ev_pos_lng - f_pos_lng, 2) < 0.000001");
//        view.append(" GROUP BY id_espace_vert");
//
//        // Création vue et affichage des résultats de la vue
//        Connection connection = null;
//        try {
//            connection = DriverManager.getConnection("jdbc:sqlite:database.db");
//            Statement statement = connection.createStatement();
//            statement.executeUpdate(view.toString());
//            ResultSet rs = statement.executeQuery("select * from espaces_verts_fontaines");
//            System.out.println("> Résultats :");
//            while (rs.next())
//                System.out.println(rs.getString("id_espace_vert") + " " + rs.getString("nom"));
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                if (connection != null) connection.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
    }
}
