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

public class CreativityComputer {

  private static final String CREATIVITY_SQL = "select creativity_question_id, description"
      + " from creativity_questions_users where user_id = ?";

  private static final boolean[] positiveAttribute = new boolean[30];

  private static final String positiveAttributeStr = "0,2,4,5,7,9,11,13,16,18,19,20,22,24,25,26,28,29";

  public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {

    String[] positiveIndices = positiveAttributeStr.split(",");
    for (String positiveIndex : positiveIndices) {
      positiveAttribute[Integer.parseInt(positiveIndex)] = true;
    }

    Properties props = new Properties();
    try (InputStream inStream = CreativityComputer.class
        .getResourceAsStream("/application.properties")) {

      props.load(inStream);
      Class.forName(props.getProperty("jdbc.driverClassName"));

      try (Connection conn = DriverManager.getConnection(props.getProperty("jdbc.url") + "?user="
          + props.getProperty("jdbc.username") + "&password=" + props.getProperty("jdbc.password"))) {

        Set<Integer> idSet = getUserIds(conn);
        Map<Integer, Double> idToCreativityScore = new HashMap<Integer, Double>();

        System.out.println("id,Creativity");
        for (int id : idSet) {
          Double creativityScore = computeCreativityScore(conn, id);
          idToCreativityScore.put(id, creativityScore);
          System.out.println(id + "," + creativityScore);
        }
      }
    }
  }

  public static Set<Integer> getUserIds(Connection conn) throws SQLException {
    Set<Integer> idSet = new HashSet<Integer>();
    String query = "select distinct user_id from creativity_questions_users";
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        idSet.add(rs.getInt(1));
      }
    }
    
    Set<Integer> idSet2 = new HashSet<Integer>();
    String query2 = "select id from users where completion_code is not null";
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query2)) {
      while (rs.next()) {
        idSet2.add(rs.getInt(1));
      }
    }

    idSet.retainAll(idSet2);

    return idSet;
  }
  
  public static Double computeCreativityScore(Connection conn, int userId) throws SQLException {
    Double creativityScore;
    try (PreparedStatement prepdStmt = conn.prepareStatement(CREATIVITY_SQL)) {
      prepdStmt.setInt(1, userId);
      try (ResultSet rs = prepdStmt.executeQuery()) {
        Integer[] rawScores = new Integer[30];
        while (rs.next()) {
          Integer questionId = rs.getInt(1);
          Integer score = Integer.parseInt(rs.getString(2));
          rawScores[questionId - 1] = score;
        }
        creativityScore = computeCreativityScore(rawScores);
      }
    }
    return creativityScore;
  }

  public static Double computeCreativityScore(Integer[] rawScores) {
    Double creativityScore = 0.0;

    if (rawScores.length != 30) {
      throw new IllegalArgumentException("Length = " + rawScores.length
          + ". There must be 30 scores from the mini IPIP test");
    }
    for (int i = 0; i < 30; i++) {
      if (rawScores[i] < 1 || rawScores[i] > 5) {
        throw new IllegalArgumentException("creativityScores[" + i + "] = " + rawScores[i]
            + ". All scores must be in the 1 to 5 range");
      }
    }

    for (int i = 0; i < 30; i++) {
      creativityScore += positiveAttribute[i] ? rawScores[i] : (6 - rawScores[i]);
    }

    return (creativityScore / 30);
  }
}
