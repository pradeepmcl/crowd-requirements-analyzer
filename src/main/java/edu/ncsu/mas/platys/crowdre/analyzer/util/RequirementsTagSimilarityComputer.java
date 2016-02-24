package edu.ncsu.mas.platys.crowdre.analyzer.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

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

  public Map<String, Double> getTagsIdf(String domain) throws SQLException {
    String tagsSelectQuery = "select stemmed_tag, count(stemmed_tag)"
        + " from requirements_tags tags, requirements reqs"
        + " where reqs.application_domain = ? and tags.requirement_id = reqs.id"
        + " group by stemmed_tag";

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

    int reqCount = getNumRequirements(domain);
    for (String tag : tagsToDf.keySet()) {
      tagsToIdf.put(tag, Math.log((double) reqCount / tagsToDf.get(tag)));
    }

    return tagsToIdf;
  }

  public Map<Integer, List<String>> getTags(String domain) throws SQLException {
    String tagsSelectQuery = "select requirement_id, stemmed_tag"
        + " from requirements_tags tags, requirements reqs"
        + " where reqs.application_domain = ? and tags.requirement_id = reqs.id";

    Map<Integer, List<String>> reqIdToTags = new HashMap<Integer, List<String>>();
    try (PreparedStatement pStmt = mConn.prepareStatement(tagsSelectQuery)) {
      pStmt.setString(1, domain);
      try (ResultSet rs = pStmt.executeQuery()) {
        while (rs.next()) {
          Integer reqId = rs.getInt(1);
          List<String> tags = reqIdToTags.get(reqId);
          if (tags == null) {
            tags = new ArrayList<String>();
            reqIdToTags.put(reqId, tags);
          }
          tags.add(rs.getString(2));
        }
      }
    }
    return reqIdToTags;
  }

  public Double computeTagSimilarity(List<String> tags1, List<String> tags2,
      Map<String, Double> tagsToIdf) {
    Double simScore = 0.0;
    for (String tag1 : tags1) {
      if (tags2.contains(tag1)) {
        simScore += tagsToIdf.get(tag1);
      }
    }
    return simScore;
  }

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException,
      Exception {
    
    String domain = args[0];
    String outFilename = args[1];

    try (RequirementsTagSimilarityComputer simComputer = new RequirementsTagSimilarityComputer();
        PrintWriter writer = new PrintWriter(outFilename, "UTF-8");) {

      Map<String, Double> tagsToIdf = simComputer.getTagsIdf(domain);
      Map<Integer, List<String>> reqIdToTags = simComputer.getTags(domain);
      List<Integer> reqIds = new ArrayList<Integer>();
      reqIds.addAll(reqIdToTags.keySet());

      Multimap<Integer, Integer> similarReqs = ArrayListMultimap.create();
      
      for (int i = 0; i < reqIds.size(); i++) {
        similarReqs.put(reqIds.get(i), reqIds.get(i));
        for (int j = i + 1; j < reqIds.size(); j++) {
          Double similarity = simComputer.computeTagSimilarity(reqIdToTags.get(reqIds.get(i)),
              reqIdToTags.get(reqIds.get(j)), tagsToIdf);
          if (similarity > 0.0) {
            similarReqs.put(reqIds.get(i), reqIds.get(j));
          }
        }
      }
      
      List<Integer> clusteredReqIds = new ArrayList<Integer>();
      List<List<Integer>> clusters = new ArrayList<List<Integer>>();
      
      for (Integer reqId : similarReqs.keys()) {
        if (!clusteredReqIds.contains(reqId)) {
          List<Integer> cluster = new ArrayList<Integer>();
          cluster.addAll(similarReqs.get(reqId));
          clusters.add(cluster);
          clusteredReqIds.addAll(similarReqs.get(reqId));
        }
      }
      
      System.out.println(clusters);
    }
  }
}
