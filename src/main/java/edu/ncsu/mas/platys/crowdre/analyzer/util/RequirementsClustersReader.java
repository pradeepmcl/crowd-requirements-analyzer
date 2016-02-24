package edu.ncsu.mas.platys.crowdre.analyzer.util;

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
import java.util.Properties;

import com.opencsv.CSVWriter;

public class RequirementsClustersReader implements AutoCloseable {

  private final Properties mProps = new Properties();

  private final Connection mConn;

  public RequirementsClustersReader() throws ClassNotFoundException, SQLException,
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

  public void writeNaiveClusteredReqs(String clustersInFilename, String reqsOutFilename)
      throws FileNotFoundException, IOException, SQLException {

    String reqsSelect = "select id, user_id, role, feature, benefit, tags"
        + " from requirements where id in (#ids#)";

    try (BufferedReader clustersBr = new BufferedReader(new FileReader(clustersInFilename));
        CSVWriter reqsWriter = new CSVWriter(new FileWriter(reqsOutFilename));
        Statement stmt = mConn.createStatement();) {

      String line;
      while ((line = clustersBr.readLine()) != null) {
        String prepdReqsSelect = reqsSelect.replace("#ids#", line);
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
  
  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException,
      Exception {
    
    String clustersSource = args[0];
    String clustersInFilename = args[1];
    String reqsOutFilename = args[2];

    try (RequirementsClustersReader clusterReader = new RequirementsClustersReader()) {
      if (clustersSource.equals("naive")) {
        clusterReader.writeNaiveClusteredReqs(clustersInFilename, reqsOutFilename);
      } else if (clustersSource.equals("matlab")) {
        // String reqIdsInFilename = args[3];
        // TODO
      }
    }
  }
}
