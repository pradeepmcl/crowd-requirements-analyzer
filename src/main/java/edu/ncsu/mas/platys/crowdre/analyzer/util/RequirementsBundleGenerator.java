package edu.ncsu.mas.platys.crowdre.analyzer.util;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class RequirementsBundleGenerator implements AutoCloseable {

  private final Properties mProps = new Properties();

  private final Connection mConn;

  public RequirementsBundleGenerator() throws ClassNotFoundException, SQLException, IOException {
    try (InputStream inStream = RequirementsBundleGenerator.class
        .getResourceAsStream("/application.properties")) {

      mProps.load(inStream);
      Class.forName(mProps.getProperty("jdbc.driverClassName"));

      mConn = DriverManager.getConnection(
          mProps.getProperty("jdbc.url") + "?user=" + mProps.getProperty("jdbc.username")
              + "&password=" + mProps.getProperty("jdbc.password"));
    }
  }

  @Override
  public void close() throws Exception {
    mConn.close();
  }

  private Map<String, List<Integer>> getRequirementIds() throws SQLException {
    Map<String, List<Integer>> reqDomainToIds = new HashMap<String, List<Integer>>();
    String tagsSelectQuery = "select requirements.id, application_domain"
        + " from requirements, users" + " where show_other != 0 and completion_code is not null"
        + "   and requirements.user_id = users.id" + " order by requirements.id";

    try (Statement stmt = mConn.createStatement();
        ResultSet rs = stmt.executeQuery(tagsSelectQuery)) {
      while (rs.next()) {
        List<Integer> reqIds = reqDomainToIds.get(rs.getString(2));
        if (reqIds == null) {
          reqIds = new ArrayList<Integer>();
          reqDomainToIds.put(rs.getString(2), reqIds);
        }
        reqIds.add(rs.getInt(1));
      }
    }
    return reqDomainToIds;
  }

  private void saveBundles(List<Map<String, List<Integer>>> bundles) throws SQLException {
    String insertQuery = "insert into requirements_bundles (application_domain_1, req_ids_1,"
        + " application_domain_2, req_ids_2, application_domain_3, req_ids_3)"
        + " values (?, ?, ?, ?, ?, ?)";

    try (PreparedStatement pStmt = mConn.prepareStatement(insertQuery)) {
      for (Map<String, List<Integer>> bundle : bundles) {
        if (bundle.size() != 3) {
          throw new IllegalArgumentException("Every map should have three elements");
        }

        int i = 1;
        for (String domainName : bundle.keySet()) {
          List<Integer> ids = bundle.get(domainName);
          if (ids.size() != 10) {
            throw new IllegalArgumentException("Every list should have ten elements");
          }
          StringBuffer idsBuffer = new StringBuffer();
          for (Integer id : ids) {
            idsBuffer.append(id + ",");
          }
          idsBuffer.replace(idsBuffer.length() - 1, idsBuffer.length(), "");
          pStmt.setString(i, domainName);
          pStmt.setString(i + 1, idsBuffer.toString());
          i += 2;
        }
        pStmt.addBatch();
      }
      pStmt.executeBatch();
    }
  }

  private void saveLeftOvers(Map<String, List<Integer>> reqDomainToIds,
      Map<String, List<Integer>> leftOverDomainToIds, String leftOversOutFilename)
          throws IOException {
    try (PrintWriter writer = new PrintWriter(new FileWriter(leftOversOutFilename))) {
      saveLeftOvers(writer, reqDomainToIds);
      saveLeftOvers(writer, leftOverDomainToIds);
    }
  }
  
  private void saveLeftOvers(PrintWriter writer, Map<String, List<Integer>> domainToIds) {
    for (String domainName : domainToIds.keySet()) {
      writer.print(domainName);
      List<Integer> reqIds = domainToIds.get(domainName);
      for (Integer reqId : reqIds) {
        writer.print("," + reqId);
      }
      writer.println();
    }
  }

  public static void main(String[] args)
      throws ClassNotFoundException, SQLException, IOException, Exception {

    String leftOversOutFilename = args[0];
    
    try (RequirementsBundleGenerator bundleGen = new RequirementsBundleGenerator()) {
      int domainId = 0;
      List<Integer> domainIds = new ArrayList<Integer>();
      List<String> domainNames = new ArrayList<String>();

      Random rand = new Random();

      Map<String, List<Integer>> reqDomainToIds = bundleGen.getRequirementIds();
      Map<String, List<Integer>> leftOverDomainToIds = new HashMap<String, List<Integer>>();

      List<Map<String, List<Integer>>> bundles = new ArrayList<Map<String, List<Integer>>>();

      for (String domainName : reqDomainToIds.keySet()) {
        System.out.println(domainName + ": " + reqDomainToIds.get(domainName).size());
        domainNames.add(domainName);
        for (int i = 0; i < reqDomainToIds.get(domainName).size(); i++) {
          domainIds.add(domainId);
        }
        if (domainName.equals("Safety")) { // Empirical tuning
          for (int i = 0; i < 100; i++) {
            domainIds.add(domainId);
          }          
        }
        domainId++;
      }

      while (reqDomainToIds.size() >= 3) {
        Map<String, List<Integer>> curMap = new HashMap<String, List<Integer>>();
        List<Integer> curIds = new ArrayList<Integer>();
        while (curIds.size() < 3) {
          Integer nextRandId = rand.nextInt(domainIds.size());
          Integer nextDomainId = domainIds.get(nextRandId);
          if (!curIds.contains(nextDomainId)) {
            curIds.add(nextDomainId);
            domainIds.remove(nextDomainId);
          }
        }
        for (Integer curId : curIds) {
          String domainName = domainNames.get(curId);
          List<Integer> domainReqIds = reqDomainToIds.get(domainName);

          if (domainReqIds.size() < 10) {
            throw new IllegalStateException("Size of domainReIds less than ten");
          }

          List<Integer> curReqIds = new ArrayList<Integer>();
          while (curReqIds.size() < 10) {
            Integer nextReqId = domainReqIds.get(rand.nextInt(domainReqIds.size()));
            curReqIds.add(nextReqId);
            domainReqIds.remove(nextReqId);
          }

          if (domainReqIds.size() < 10) {
            leftOverDomainToIds.put(domainName, domainReqIds);
            reqDomainToIds.remove(domainName);
            domainIds.removeAll(Collections.singleton(domainNames.indexOf(domainName)));
          }
          curMap.put(domainName, curReqIds);
        }
        bundles.add(curMap);
      }

      System.out.println(bundles.size());

      System.out.println("Left overs 1");
      for (String domainName : reqDomainToIds.keySet()) {
        System.out.println(domainName + ": " + reqDomainToIds.get(domainName).size());
      }

      System.out.println("Left overs 2");
      for (String domainName : leftOverDomainToIds.keySet()) {
        System.out.println(domainName + ": " + leftOverDomainToIds.get(domainName).size());
      }

      System.out.print("Save bundles? [yes/no]: ");
      String input = System.console().readLine();
      if (input.trim().equalsIgnoreCase("yes")) {
        bundleGen.saveBundles(bundles);
        bundleGen.saveLeftOvers(reqDomainToIds, leftOverDomainToIds, leftOversOutFilename);
      }
    }
  }
}
