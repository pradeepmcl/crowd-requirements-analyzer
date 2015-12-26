package edu.ncsu.mas.platys.crowdre.analyzer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class PersonalityComputer {

  private static final String PERSONALITY_SQL = "select personality_question_id, description"
      + " from personality_questions_users where user_id = ?";

  public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
    Properties props = new Properties();
    try (InputStream inStream = new FileInputStream("src/main/resources/application.properties");
        Connection conn = DriverManager.getConnection(props.getProperty("jdbc.url") + "?user="
            + props.getProperty("jdbc.username") + "&password="
            + props.getProperty("jdbc.password"))) {
      
      props.load(inStream);
      Class.forName(props.getProperty("jdbc.driverClassName"));

      Double[] traits = computePeronalityTraits(conn, 30);
      System.out.println("E = " + traits[0] + "; A = " + traits[1] + "; C = " + traits[2]
          + "; N = " + traits[3] + "; I = " + traits[4]);
    }
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
      traits[i] = (personalityScores[i].doubleValue() + (5 - personalityScores[i + 5])
          + personalityScores[i + 10] + (5 - personalityScores[i + 15])) / 4;
    }

    return traits;
  }
}
