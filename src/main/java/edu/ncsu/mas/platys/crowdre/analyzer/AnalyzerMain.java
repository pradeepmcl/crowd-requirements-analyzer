package edu.ncsu.mas.platys.crowdre.analyzer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class AnalyzerMain implements AutoCloseable {

  private final Properties props = new Properties();

  private final Connection mConn;

  public AnalyzerMain() throws ClassNotFoundException, IOException, SQLException {
    try (InputStream inStream = new FileInputStream("src/main/resources/application.properties")) {
      props.load(inStream);
      Class.forName(props.getProperty("jdbc.driverClassName"));

      mConn = DriverManager.getConnection(props.getProperty("jdbc.url") + "?user="
          + props.getProperty("jdbc.username") + "&password=" + props.getProperty("jdbc.password"));
      // TODO
    }
  }

  @Override
  public void close() throws SQLException {
    mConn.close();
  }

  public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
    try (AnalyzerMain analyzerMain = new AnalyzerMain()) {
      
    }
  }

  public Map<Integer, Integer> getUserIdToBatch(Set<Integer> batchIds) throws SQLException {
    StringBuffer queryBuffer = new StringBuffer();
    queryBuffer.append("select id, created_batch from users where created_batch is not null");
    
    if (batchIds != null && batchIds.size() >= 1) {
      queryBuffer.append(" and created_batch in (");
      for (Integer batchId : batchIds) {
        queryBuffer.append(batchId + ",");
      }
      queryBuffer.replace(queryBuffer.length() - 1, queryBuffer.length(), ")");
    }
    
    Map<Integer, Integer> userToBatchMap = new HashMap<Integer, Integer>();

    try (Statement stmt = mConn.createStatement();
        ResultSet rs = stmt.executeQuery(queryBuffer.toString())) {
      while (rs.next()) {
        userToBatchMap.put(rs.getInt(1), rs.getInt(2));
      }
    }
    
    return userToBatchMap;
  }
  
  public void readRatings() {
    
  }
}
