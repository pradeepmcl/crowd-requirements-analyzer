package edu.ncsu.mas.platys.crowdre.analyzer.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import edu.ncsu.mas.platys.crowdre.analyzer.util.nlp.WordnetSimilarityComputer;

public class RequirementsWordnetSimilarityComputer {
  
  public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
    Properties props = new Properties();
    try (InputStream inStream = new FileInputStream("src/main/resources/application.properties")) {
      props.load(inStream);
      Class.forName(props.getProperty("jdbc.driverClassName"));

      try (Connection conn = DriverManager.getConnection(props.getProperty("jdbc.url") + "?user="
          + props.getProperty("jdbc.username") + "&password=" + props.getProperty("jdbc.password"));
          PrintWriter writer = new PrintWriter("feature-similarity.csv", "UTF-8");) {

        WordnetSimilarityComputer simComp = new WordnetSimilarityComputer("src/main/resources/stoplist.txt");
        Map<Integer, String> features = getReqFeatures(conn);
        for (Integer reqId1 : features.keySet()) {
          for (Integer reqId2 : features.keySet()) {
            if (reqId1 != reqId2)
              writer.println(reqId1 + "," + reqId2 + ","
                  + simComp.computeSentenceSimilarity(features.get(reqId1), features.get(reqId2)));
          }
        }
      }
    }
  }

  public static Map<Integer, String> getReqFeatures(Connection conn) throws SQLException {
    Map<Integer, String> features = new HashMap<Integer, String>();

    String query = "select id, feature from requirements";
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        features.put(rs.getInt(1), rs.getString(2));
      }
    }
    return features;
  }
}
