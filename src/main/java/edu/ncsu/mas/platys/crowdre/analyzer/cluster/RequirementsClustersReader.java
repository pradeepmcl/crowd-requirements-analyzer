package edu.ncsu.mas.platys.crowdre.analyzer.cluster;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.opencsv.CSVWriter;

public class RequirementsClustersReader implements AutoCloseable {

  private final Properties mProps = new Properties();

  private final Connection mConn;

  public RequirementsClustersReader() throws ClassNotFoundException, SQLException,
      IOException {
    try (InputStream inStream = RequirementsClustersReader.class
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

  public int getNumRequirements(String domain) throws SQLException {
    String countQuery = "select count(*) from requirements where application_domain = ?";
    int count = -1;
    try (PreparedStatement pStmt = mConn.prepareStatement(countQuery)) {
      pStmt.setString(1, domain);
      try (ResultSet rs = pStmt.executeQuery()) {
        if (rs.next()) {
          count = rs.getInt(1);
        }
      }
    }
    return count;
  }
  
  public void writeClusteredReqs(List<String> clusters, String reqsOutFilename)
      throws FileNotFoundException, IOException, SQLException {

    String reqsSelect = "select id, user_id, role, feature, benefit, tags"
        + " from requirements where id in (#ids#)";

    try (CSVWriter reqsWriter = new CSVWriter(new FileWriter(reqsOutFilename));
        Statement stmt = mConn.createStatement();) {
      for (String cluster : clusters) {
        String prepdReqsSelect = reqsSelect.replace("#ids#", cluster);
        try (ResultSet rs = stmt.executeQuery(prepdReqsSelect)) {
          /*while (rs.next()) {
            reqsWriter.println(rs.getInt(1) + "," + rs.getInt(2) + "," + rs.getString(3) + ","
                + rs.getString(4) + "," + rs.getString(5) + "," + rs.getString(6));
          }*/
          reqsWriter.writeAll(rs, false);
          reqsWriter.writeNext(new String[] {});
        }
      }
    }
  }
  
  private List<String> readNaiveClusters(String clustersInFilename)
      throws FileNotFoundException, IOException {
    List<String> clusters = new ArrayList<String>();
    try (BufferedReader clustersBr = new BufferedReader(new FileReader(clustersInFilename))) {
      String line;
      while ((line = clustersBr.readLine()) != null) {
        clusters.add(line);
      }
    }
    return clusters;
  }
  
  private List<String> readMatlabClusters(String clustersInFilename, String reqIdsInFilename)
      throws FileNotFoundException, IOException {
    
    List<Integer> reqIds = new ArrayList<Integer>();
    try (BufferedReader reqIdsBr = new BufferedReader(new FileReader(reqIdsInFilename))) {
      String line;
      while ((line = reqIdsBr.readLine()) != null) {
        reqIds.add(Integer.parseInt(line));
      }
    }

    Map<Integer, String> clusterIdTOReqIds = new HashMap<Integer, String>();
    try (BufferedReader clustersBr = new BufferedReader(new FileReader(clustersInFilename))) {
      String line;
      int i = 0;
      while ((line = clustersBr.readLine()) != null) {
        Integer clusterId = Integer.parseInt(line);
        
        String ids = clusterIdTOReqIds.get(clusterId);
        if (ids == null) {
          clusterIdTOReqIds.put(clusterId, reqIds.get(i).toString());
        } else {
          clusterIdTOReqIds.put(clusterId, ids + "," + reqIds.get(i));
        }
        
        i++;
      }
    }
    
    List<String> clusters = new ArrayList<String>();
    clusters.addAll(clusterIdTOReqIds.values());
    return clusters;
  }
  
  public static void main(String[] args)
      throws ClassNotFoundException, SQLException, IOException, Exception {

    String clustersInFileFormat = args[0];
    String clustersInFilename = args[1];
    String reqsOutFilename = args[2];

    try (RequirementsClustersReader clusterReader = new RequirementsClustersReader()) {
      if (clustersInFileFormat.equals("naive")) {
        List<String> clusters = clusterReader.readNaiveClusters(clustersInFilename);
        clusterReader.writeClusteredReqs(clusters, reqsOutFilename);
      } else if (clustersInFileFormat.equals("matlab")) {
        String reqIdsInFilename = args[3];
        List<String> clusters = clusterReader.readMatlabClusters(clustersInFilename,
            reqIdsInFilename);
        clusterReader.writeClusteredReqs(clusters, reqsOutFilename);
      }
    }
  }
}
