package edu.ncsu.mas.platys.crowdre.analyzer.tag;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class RequirementsTagSimilarityComputer implements AutoCloseable {

  private final Properties mProps = new Properties();

  private final Connection mConn;

  public RequirementsTagSimilarityComputer() throws ClassNotFoundException, SQLException,
      IOException {
    try (InputStream inStream = RequirementsTagSimilarityComputer.class
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

  public Multimap<Integer, Integer> computeTagSimilarity(String domain, String reqIdsOutFilename,
      String similarityOutFilename)
          throws FileNotFoundException, UnsupportedEncodingException, SQLException {

    Multimap<Integer, Integer> similarReqs = ArrayListMultimap.create();

    try (PrintWriter reqIdsWriter = new PrintWriter(reqIdsOutFilename, "UTF-8");
        PrintWriter pdistWriter = new PrintWriter(similarityOutFilename, "UTF-8");) {

      Map<String, Double> tagsToIdf = getTagsIdf(domain);
      Map<Integer, List<String>> reqIdToTags = getTags(domain);
      List<Integer> reqIds = new ArrayList<Integer>();
      reqIds.addAll(reqIdToTags.keySet());

      for (int i = 0; i < reqIds.size(); i++) {
        similarReqs.put(reqIds.get(i), reqIds.get(i));
        reqIdsWriter.println(reqIds.get(i));
        for (int j = i + 1; j < reqIds.size(); j++) {
          Double similarity = computeTagSimilarity(reqIdToTags.get(reqIds.get(i)),
              reqIdToTags.get(reqIds.get(j)), tagsToIdf);
          pdistWriter.println(similarity);
          if (similarity > 0.0) {
            similarReqs.put(reqIds.get(i), reqIds.get(j));
          }
        }
      }
    }
    return similarReqs;
  }
  
  public void performNaiveClustering(Multimap<Integer, Integer> similarReqs,
      String clustersOutFilename) throws FileNotFoundException, UnsupportedEncodingException {
    Set<Integer> clusteredReqIds = new HashSet<Integer>();
    List<Set<Integer>> clusters = new ArrayList<Set<Integer>>();

    for (Integer reqId : similarReqs.keys()) {
      if (!clusteredReqIds.contains(reqId)) {
        Set<Integer> cluster = new HashSet<Integer>();
        cluster.addAll(similarReqs.get(reqId));
        clusters.add(cluster);
        clusteredReqIds.addAll(similarReqs.get(reqId));
      }
    }

    System.out.println("Number of clusters: " + clusters.size() + "; Total number of requirements: "
        + clusteredReqIds.size());
    
    try (PrintWriter clustersWriter = new PrintWriter(clustersOutFilename, "UTF-8")) {
      for (int i = 0; i < clusters.size(); i++) {
        Set<Integer> cluster = clusters.get(i);
        StringBuffer strBuffer = new StringBuffer();
        for (Integer id : cluster) {
          strBuffer.append(id + ",");
        }
        strBuffer.replace(strBuffer.length() - 1, strBuffer.length(), "");
        clustersWriter.println(strBuffer.toString());
      }
    }
  }
  
  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException,
      Exception {
    
    String domain = args[0];
    String reqIdsOutFilename = args[1];
    String similarityOutFilename = args[2];
    String clustersOutFilename = args[3];

    try (RequirementsTagSimilarityComputer simComputer = new RequirementsTagSimilarityComputer()) {

      Multimap<Integer, Integer> similarReqs = simComputer.computeTagSimilarity(domain,
          reqIdsOutFilename, similarityOutFilename);
      
      simComputer.performNaiveClustering(similarReqs, clustersOutFilename);
    }
  }
}
