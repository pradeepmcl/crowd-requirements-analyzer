package edu.ncsu.mas.platys.crowdre.analyzer.vectorspace;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class RequirementsVectorSpaceSimilarityComputer implements AutoCloseable {

  private final Properties mProps = new Properties();

  private final Connection mConn;

  // TF-IDF table
  private Table<Integer, String, Double> tfIdf = HashBasedTable.create();

  public RequirementsVectorSpaceSimilarityComputer() throws ClassNotFoundException, SQLException,
      IOException {
    
    try (InputStream inStream = RequirementsVectorSpaceSimilarityComputer.class
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
  
  private void readTfIdfFomFile(String inTfIdfFilename) throws IOException {
    try (BufferedReader tfIdfsBr = new BufferedReader(new FileReader(inTfIdfFilename))) {
      String line;
      while ((line = tfIdfsBr.readLine()) != null) {
        String[] lineParts = line.split("\\|");
        tfIdf.put(Integer.parseInt(lineParts[0]), lineParts[1], Double.parseDouble(lineParts[2]));
      }
    }
  }
  
  private void computeSimilarities(String reqIdsOutFilename, String similarityOutFilename)
      throws FileNotFoundException, UnsupportedEncodingException {
    List<Integer> reqIds = new ArrayList<Integer>();
    reqIds.addAll(tfIdf.rowKeySet());

    try (PrintWriter reqIdsWriter = new PrintWriter(reqIdsOutFilename, "UTF-8");
        PrintWriter pdistWriter = new PrintWriter(similarityOutFilename, "UTF-8")) {
      for (int i = 0; i < reqIds.size(); i++) {
        reqIdsWriter.println(reqIds.get(i));
        for (int j = i + 1; j < reqIds.size(); j++) {
          Double similarity = computeTagSimilarity(reqIds.get(i), reqIds.get(j));
          pdistWriter.println(similarity);
        }
      }
    }
  }
  
  private Double computeTagSimilarity(Integer reqId1, Integer reqId2) {
    Map<String, Double> termWeights1 = tfIdf.row(reqId1);
    Map<String, Double> termWeights2 = tfIdf.row(reqId2);
    
    Double dotProduct = 0.0;
    for (String term : termWeights1.keySet()) {
      if (termWeights2.containsKey(term)) {
        dotProduct += termWeights1.get(term) * termWeights2.get(term);
      }
    }
    
    Double cosineSim = 0.0;
    if (dotProduct > 0) {
      cosineSim = dotProduct / (getVectorLength(termWeights1) * getVectorLength(termWeights2));
    }
    return cosineSim;
  }
  
  private Double getVectorLength(Map<String, Double> termWeights1) {
    Double squaredLength = 0.0;
    for (String term : termWeights1.keySet()) {
      squaredLength += (termWeights1.get(term) * termWeights1.get(term));
    }
    return Math.sqrt(squaredLength);
  }

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException,
      Exception {

    String inTfIdfFilename = args[0];
    String reqIdsOutFilename = args[1];
    String similarityOutFilename = args[2];
    
    try (RequirementsVectorSpaceSimilarityComputer reqVSTransformer = new RequirementsVectorSpaceSimilarityComputer()) {
      reqVSTransformer.readTfIdfFomFile(inTfIdfFilename);
      reqVSTransformer.computeSimilarities(reqIdsOutFilename, similarityOutFilename);
    }
  }
}
