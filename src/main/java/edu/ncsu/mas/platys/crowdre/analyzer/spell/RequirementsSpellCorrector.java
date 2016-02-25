package edu.ncsu.mas.platys.crowdre.analyzer.spell;

import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import edu.ncsu.mas.platys.crowdre.analyzer.nlp.SpellChecker;

public class RequirementsSpellCorrector {

  public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
    Properties props = new Properties();
    try (InputStream inStream = RequirementsSpellCorrector.class
        .getResourceAsStream("/application.properties")) {
      
      props.load(inStream);
      Class.forName(props.getProperty("jdbc.driverClassName"));

      String updateQuery = "Update requirements set role = REPLACE(role, ?, ?) where id = ?";
      
      try (Connection conn = DriverManager.getConnection(
          props.getProperty("jdbc.url") + "?user=" + props.getProperty("jdbc.username")
              + "&password=" + props.getProperty("jdbc.password"));
          PreparedStatement updateStmt = conn.prepareStatement(updateQuery)) {

        SpellChecker spellChecker = new SpellChecker();

        Console c = System.console();
        if (c == null) {
          System.err.println("No console.");
          System.exit(1);
        }

        Map<Integer, String> roles = getReqRoles(conn);
        for (Integer reqId : roles.keySet()) {
          String srcPhrase = roles.get(reqId);
          Map<Integer, List<String>> suggestions = spellChecker.getSuggestions(srcPhrase);

          for (Integer col : suggestions.keySet()) {
            List<String> colSuggestions = suggestions.get(col);
            System.out.println(
                roles.get(reqId) + "; Error in " + col + "; Suggestions: " + colSuggestions); 

            String srcWordPosStr = c
                .readLine("Enter source position (starting 0)" + " or nothing to continue: ");
            if (srcWordPosStr != null & srcWordPosStr.trim().length() > 0) {
              int srcWordPos = Integer.valueOf(srcWordPosStr);
              String srcWord;
              if (srcWordPos == -1) {
                srcWord = c.readLine("Enter source word:");
              } else {
                srcWord = srcPhrase.split(" ")[srcWordPos];
              }

              String destWordPosStr = c.readLine("Enter destination position (starting 0):");
              int destWordPos = Integer.valueOf(destWordPosStr);
              String destWord;
              if (destWordPos == -1) {
                destWord = c.readLine("Enter destination word:");
              } else {
                destWord = colSuggestions.get(destWordPos);
              }
              
              updateStmt.setString(1, srcWord);
              updateStmt.setString(2, destWord);
              updateStmt.setInt(3, reqId);
              System.out.println(updateStmt);
              updateStmt.addBatch();
            }
          }
        }
        updateStmt.executeBatch();
      }
    }
  }

  public static Map<Integer, String> getReqRoles(Connection conn) throws SQLException {
    Map<Integer, String> roles = new HashMap<Integer, String>();

    String query = "select id, role from requirements";
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        roles.put(rs.getInt(1), rs.getString(2));
      }
    }
    return roles;
  }

}
