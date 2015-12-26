package edu.ncsu.mas.platys.crowdre.analyzer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class MTurkIdFilter {

  // private static final String exceptionIds = "'pmuruka','najmeri','mpradeep'";
  
  private static final String MTURK_ID_COMPLETE_SQL = "select id from users"
      + " where completion_code is not null and created_phase = ?";

  public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
    Properties props = new Properties();
    try (InputStream inStream = new FileInputStream("src/main/resources/application.properties")) {
      props.load(inStream);
      Class.forName(props.getProperty("jdbc.driverClassName"));

      try (Connection conn = DriverManager.getConnection(props.getProperty("jdbc.url") + "?user="
          + props.getProperty("jdbc.username") + "&password=" + props.getProperty("jdbc.password"))) {
        Set<Integer> idSet = getMturkIds(conn, 1, true);
        System.out.println(idSet.size());
        System.out.println(idSet);
      }
    }
  }

  public static Set<Integer> getMturkIds(Connection conn, int phase, boolean isComplete)
      throws SQLException {
    Set<Integer> idSet = new HashSet<Integer>();
    
    String query;
    if (isComplete) {
      query = MTURK_ID_COMPLETE_SQL;
    } else {
      query = MTURK_ID_COMPLETE_SQL; // TODO
    }
    
    try (PreparedStatement prepdStmt = conn.prepareStatement(query)) {
      prepdStmt.setInt(1, phase);
      try (ResultSet rs = prepdStmt.executeQuery()) {
        while (rs.next()) {
          idSet.add(rs.getInt(1));
        }
      }
    }
    return idSet;
  }
}
