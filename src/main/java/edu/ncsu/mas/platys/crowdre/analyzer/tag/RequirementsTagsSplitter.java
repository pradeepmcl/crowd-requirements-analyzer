package edu.ncsu.mas.platys.crowdre.analyzer.tag;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import opennlp.tools.stemmer.PorterStemmer;
import opennlp.tools.stemmer.Stemmer;

public class RequirementsTagsSplitter implements AutoCloseable {

  private final Properties mProps = new Properties();

  private final Connection mConn;

  public RequirementsTagsSplitter() throws ClassNotFoundException, SQLException, IOException {
    try (InputStream inStream = RequirementsTagsSplitter.class
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

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException,
      Exception {
    String insertTags = "insert into requirements_tags (tag, stemmed_tag, requirement_id) "
        + "values (?, ?, ?)";

    Stemmer stemmer = new PorterStemmer();
    
    try (RequirementsTagsSplitter splitter = new RequirementsTagsSplitter();
        PreparedStatement updateStmt = splitter.mConn.prepareStatement(insertTags)) {
      Map<Integer, String> reqIdToCSVTags = splitter.readOriginalTags();

      for (Integer reqId : reqIdToCSVTags.keySet()) {
        String csvTag = reqIdToCSVTags.get(reqId);
        String[] tags = csvTag.split(",");
        for (String tag : tags) {
          String trimmedTag = tag.trim().toLowerCase();
          
          if (trimmedTag.startsWith("#")) {
            trimmedTag = trimmedTag.substring(1, trimmedTag.length());
          }
          
          if (trimmedTag.length() > 0) {
            String stemmedTag = stemmer.stem(trimmedTag).toString();
            updateStmt.setString(1, trimmedTag);
            updateStmt.setString(2, stemmedTag);
            updateStmt.setInt(3, reqId);
            updateStmt.addBatch();
          }
        }
      }
      updateStmt.executeBatch();
    }
  }

  public Map<Integer, String> readOriginalTags() throws SQLException {
    Map<Integer, String> idToTags = new HashMap<Integer, String>();
    String tagsSelectQuery = "select id, tags from requirements";
    try (Statement stmt = mConn.createStatement();
        ResultSet rs = stmt.executeQuery(tagsSelectQuery)) {
      while (rs.next()) {
        idToTags.put(rs.getInt(1), rs.getString(2));
      }
    }
    return idToTags;
  }

}
