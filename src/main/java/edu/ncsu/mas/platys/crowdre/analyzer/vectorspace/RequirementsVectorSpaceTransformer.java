package edu.ncsu.mas.platys.crowdre.analyzer.vectorspace;

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
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class RequirementsVectorSpaceTransformer implements AutoCloseable {

  private final Properties mProps = new Properties();

  private final Connection mConn;
  
  // For TF, count terms in each document
  private Table<Integer, String, Integer> docTermCounts = HashBasedTable.create();

  // TF-IDF table
  private Table<Integer, String, Double> tfIdf = HashBasedTable.create();

  public RequirementsVectorSpaceTransformer() throws ClassNotFoundException, SQLException,
      IOException {
    
    try (InputStream inStream = RequirementsVectorSpaceTransformer.class
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
  
  private void computeTermAndDocFrequencies(String domain) throws SQLException {
    String stemmedReqsSelect = "select req_id, stemmed_role, stemmed_feature, stemmed_benefit, stemmed_tags"
        + " from stemmed_requirements r1, requirements r2"
        + " where r2.application_domain = ? and r1.req_id = r2.id";

    try (PreparedStatement pStmt = mConn.prepareStatement(stemmedReqsSelect)) {
      pStmt.setString(1, domain);
      try (ResultSet rs = pStmt.executeQuery()) {
        while (rs.next()) {
          Integer reqId = rs.getInt(1);
          Set<String> curTerms = new HashSet<String>();
          for (int i = 2; i <= 5; i++) {
            String text = rs.getString(i);
            if (text != null && !text.toLowerCase().equals("null")) {
              String[] terms = text.split("\\|");
              for (String term : terms) {
                curTerms.add(term);
                Integer termCount = docTermCounts.get(reqId, term);
                if (termCount == null) {
                  docTermCounts.put(reqId, term, 1);
                } else {
                  docTermCounts.put(reqId, term, termCount + 1);
                }
              }
            }
          }
        }
      }
    }
  }
  
  private void buildTfIdf() {
    for (Integer reqId : docTermCounts.rowKeySet()) {
      Map<String, Integer> termCounts = docTermCounts.row(reqId);
      Integer maxCount = Collections.max(termCounts.values());
      for (String term : termCounts.keySet()) {
        double tF = 0.5 + (0.5 * termCounts.get(term) / maxCount);
        Integer termDocCount = docTermCounts.column(term).size();
        double iDF = Math.log((double) docTermCounts.rowKeySet().size() / termDocCount);
        tfIdf.put(reqId, term, tF * iDF);
      }
    }
  }
  
  private void writeTfIdfToFile(String outFilename)
      throws FileNotFoundException, UnsupportedEncodingException {
    
    try (PrintWriter writer = new PrintWriter(outFilename, "UTF-8")) {
      // Write terms on the first line
      /* StringBuffer terms = new StringBuffer();
      for (String term : tfIdf.columnKeySet()) {
        terms.append(term + "|");
      }
      terms.replace(terms.length() - 1, terms.length(), "");
      writer.println(terms);*/
      
      // Write frequencies on the following lines
      for (Integer reqId : tfIdf.rowKeySet()) {
        Map<String, Double> termFreqs = tfIdf.row(reqId);
        for (String term : termFreqs.keySet()) {
          writer.println(reqId + "|" + term + "|" + termFreqs.get(term));
        }
      }
    }
  }
  
  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException,
      Exception {

    String domain = args[0];
    String outFilename = args[1];
    
    try (RequirementsVectorSpaceTransformer reqVSTransformer = new RequirementsVectorSpaceTransformer()) {
      reqVSTransformer.computeTermAndDocFrequencies(domain);
      reqVSTransformer.buildTfIdf();
      reqVSTransformer.writeTfIdfToFile(outFilename);
    }
  }
}
