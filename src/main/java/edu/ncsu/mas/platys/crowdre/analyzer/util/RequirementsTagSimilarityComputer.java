package edu.ncsu.mas.platys.crowdre.analyzer.util;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RequirementsTagSimilarityComputer implements AutoCloseable {

  private final Properties mProps = new Properties();

  private final Connection mConn;

  public RequirementsTagSimilarityComputer() throws ClassNotFoundException, SQLException,
      IOException {
    try (InputStream inStream = RequirementsSpellCorrector.class
        .getResourceAsStream("/application.properties")) {

      mProps.load(inStream);
      Class.forName(mProps.getProperty("jdbc.driverClassName"));

      mConn = DriverManager.getConnection(mProps.getProperty("jdbc.url") + "?user="
          + mProps.getProperty("jdbc.username") + "&password="
          + mProps.getProperty("jdbc.password"));
    }
  }

  @Override
  public void close() throws Exception {
    mConn.close();
  }

  public Map<String, Double> getTagsIdf(String domain) throws SQLException {
    String tagsSelectQuery = "select stemmed_tag, count(stemmed_tag)"
        + " from requirements_tags tags, requirements reqs"
        + " where reqs.application_domain = ? and tags.requirement_id = reqs.id"
        + " group by count(stemmed_tag)";
    
    Map<String, Integer> tagsToDf = new HashMap<String, Integer>();
    try (PreparedStatement pStmt = mConn.prepareStatement(tagsSelectQuery)) {
      pStmt.setString(1, domain);
      try (ResultSet rs = pStmt.executeQuery()) {
        while (rs.next()) {
          tagsToDf.put(rs.getString(1), rs.getInt(2));
        }
      }
    }
    
    Map<String, Double> tagsToIdf = new HashMap<String, Double>();
    
    int valueSum = getValueSum(tagsToDf);
    for (String tag : tagsToDf.keySet()) {
      tagsToIdf.put(tag, Math.log((double) valueSum / tagsToDf.get(tag)));
    }
    
    return tagsToIdf;
  }
  
  private static int getValueSum(Map<String, Integer> valueMap) {
    int valueSum = 0;
    for (String key : valueMap.keySet()) {
      valueSum += valueMap.get(key);
    }
    return valueSum;
  }

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException,
      Exception {
    try (RequirementsTagSimilarityComputer simComputer = new RequirementsTagSimilarityComputer()) {
      Map<String, Double> tagsToIdf = simComputer.getTagsIdf("Health");
      System.out.println(tagsToIdf);
    }
  }
}
