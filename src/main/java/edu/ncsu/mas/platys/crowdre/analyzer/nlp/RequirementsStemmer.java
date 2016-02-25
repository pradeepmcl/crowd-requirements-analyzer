package edu.ncsu.mas.platys.crowdre.analyzer.nlp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class RequirementsStemmer implements AutoCloseable {

  private final Properties mProps = new Properties();

  private final Connection mConn;
  
  private final OpenNLPSingleton nlp;
  
  private final Set<String> stopWords = new HashSet<String>();

  public RequirementsStemmer() throws ClassNotFoundException, SQLException,
      IOException {
    
    try (InputStream inStream = RequirementsStemmer.class
        .getResourceAsStream("/application.properties")) {

      mProps.load(inStream);
      Class.forName(mProps.getProperty("jdbc.driverClassName"));

      mConn = DriverManager.getConnection(mProps.getProperty("jdbc.url") + "?user="
          + mProps.getProperty("jdbc.username") + "&password="
          + mProps.getProperty("jdbc.password"));
    }
    
    nlp = OpenNLPSingleton.INSTANCE;
    
    try (BufferedReader stopWordsReader = new BufferedReader(new InputStreamReader(
        OpenNLPSingleton.class.getResourceAsStream("/stoplist.txt")))) {
      
      String line;
      while ((line = stopWordsReader.readLine()) != null) {
        stopWords.add(line);
      }
    }
  }

  @Override
  public void close() throws Exception {
    mConn.close();
  }
  
  private boolean filterByPos(String pennTreePosTag) {
    if (pennTreePosTag.indexOf("NN") == 0) {
      return true;
    }
    if (pennTreePosTag.indexOf("VB") == 0) {
      return true;
    }
    if (pennTreePosTag.indexOf("JJ") == 0) {
      return true;
    }
    if (pennTreePosTag.indexOf("RB") == 0) {
      return true;
    }
    return false;
  }
  
  private List<Integer> filterWords(String[] words, String[] posTags) {
    List<Integer> indicesToRetain = new ArrayList<Integer>();
    for (int i = 0; i < words.length; i++) {
      if (words[i].length() < 3 || stopWords.contains(words[i]) || !filterByPos(posTags[i])) {
        continue;
      }
      indicesToRetain.add(i);
    }
    return indicesToRetain;
  }
  
  private void stemRequirements() throws SQLException {
    String tagsSelectQuery = "select id, role, feature, benefit, tags from requirements";
    String insertQuery = "insert into stemmed_requirements"
        + " (req_id, stemmed_role, stemmed_feature, stemmed_benefit, stemmed_tags) values"
        + " (?, ?, ?, ?, ?)";
    
    try (Statement stmt = mConn.createStatement();
        ResultSet rs = stmt.executeQuery(tagsSelectQuery);
        PreparedStatement pStmt = mConn.prepareStatement(insertQuery)) {
      
      while (rs.next()) {
        Integer reqId = rs.getInt(1);
        pStmt.setInt(1, reqId);
        
        for (int i = 2; i <= 5; i++) {
          String text = rs.getString(i).trim().toLowerCase();
          text.replace("#", ""); // Some tags have #

          if (text.length() > 0) {
            String[] words = nlp.tokenize(text);
            String[] posTags = nlp.postag(words);

            List<Integer> indicesToRetain = filterWords(words, posTags);

            StringBuffer stemmedWords = new StringBuffer();
            for (int j = 0; j < indicesToRetain.size(); j++) {
              stemmedWords.append(nlp.porterStem(words[indicesToRetain.get(j)]).toString() + "|");
            }
            if (stemmedWords.length() > 0) { 
              stemmedWords.replace(stemmedWords.length() - 1, stemmedWords.length(), "");
              pStmt.setString(i, stemmedWords.toString());
            } else {
              pStmt.setString(i, null);
            }
          } else {
            pStmt.setString(i, null);
          }
        }
        pStmt.addBatch();
      }
      pStmt.executeBatch();
    }
  }
  
  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException,
      Exception {

    try (RequirementsStemmer reqStemmer = new RequirementsStemmer()) {
      reqStemmer.stemRequirements();
    }
  }
}
