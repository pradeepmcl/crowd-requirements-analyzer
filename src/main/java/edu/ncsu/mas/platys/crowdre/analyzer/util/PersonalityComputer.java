package edu.ncsu.mas.platys.crowdre.analyzer.util;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class PersonalityComputer {

  private static final String PERSONALITY_SQL = "select personality_question_id, description"
      + " from personality_questions_users where user_id = ?";

  public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
    Properties props = new Properties();
    try (InputStream inStream = PersonalityComputer.class
        .getResourceAsStream("/application.properties")) {

      props.load(inStream);
      Class.forName(props.getProperty("jdbc.driverClassName"));

      try (Connection conn = DriverManager.getConnection(props.getProperty("jdbc.url") + "?user="
          + props.getProperty("jdbc.username") + "&password=" + props.getProperty("jdbc.password"))) {

        Set<Integer> idSet = getUserIds(conn);
        Map<Integer, Double[]> idToTraits = new HashMap<Integer, Double[]>();

        System.out.println("id,E,A,C,N,I");
        for (int id : idSet) {
          Double[] traits = computePeronalityTraits(conn, id);
          idToTraits.put(id, traits);
          System.out.println(id + "," + traits[0] + "," + traits[1] + "," + traits[2] + ","
              + traits[3] + "," + traits[4]);
        }

        System.out.println("Distances for the first id...");
        for (int id1 : idSet) {
          for (int id2 : idSet) {
            System.out.print(getPersonalityEuclideanDistance(idToTraits.get(id1),
                idToTraits.get(id2))
                + " ");
          }
          break; // One iteration is sufficient for testing
        }
      }
    }
  }

  public static Set<Integer> getUserIds(Connection conn) throws SQLException {
    Set<Integer> idSet1 = new HashSet<Integer>();
    String query1 = "select distinct user_id from personality_questions_users";
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query1)) {
      while (rs.next()) {
        idSet1.add(rs.getInt(1));
      }
    }
    
    Set<Integer> idSet2 = new HashSet<Integer>();
    String query2 = "select id from users where completion_code is not null";
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query2)) {
      while (rs.next()) {
        idSet2.add(rs.getInt(1));
      }
    }

    idSet1.retainAll(idSet2);
    return idSet1;
  }

  public static Double[] computePeronalityTraits(Connection conn, int userId) throws SQLException {
    Double[] traits;
    try (PreparedStatement prepdStmt = conn.prepareStatement(PERSONALITY_SQL)) {
      prepdStmt.setInt(1, userId);
      try (ResultSet rs = prepdStmt.executeQuery()) {
        Integer[] personalityScores = new Integer[20];
        while (rs.next()) {
          Integer questionId = rs.getInt(1);
          Integer score = Integer.parseInt(rs.getString(2));
          personalityScores[questionId - 1] = score;
        }
        traits = computePeronalityTraits(personalityScores);
      }
    }
    return traits;
  }

  public static Double[] computePeronalityTraits(Integer[] personalityScores) {
    Double[] traits = new Double[5];

    if (personalityScores.length != 20) {
      throw new IllegalArgumentException("Length = " + personalityScores.length
          + ". There must be 20 scores from the mini IPIP test");
    }
    for (int i = 0; i < 20; i++) {
      if (personalityScores[i] < 1 || personalityScores[i] > 5) {
        throw new IllegalArgumentException("personalityScores[" + i + "] = " + personalityScores[i]
            + ". All scores must be in the 1 to 5 range");
      }
    }

    for (int i = 0; i < 5; i++) {
      traits[i] = (personalityScores[i].doubleValue() + (6 - personalityScores[i + 5])
          + personalityScores[i + 10] + (6 - personalityScores[i + 15])) / 4;
    }

    return traits;
  }

  public static Double getPersonalityEuclideanDistance(Double[] trait1, Double[] trait2) {
    Double squaredDistance = 0.0;
    for (int i = 0; i < 5; i++) {
      squaredDistance += (trait1[i] - trait2[i]) * (trait1[i] - trait2[i]);
    }
    return Math.sqrt(squaredDistance);
  }
}
