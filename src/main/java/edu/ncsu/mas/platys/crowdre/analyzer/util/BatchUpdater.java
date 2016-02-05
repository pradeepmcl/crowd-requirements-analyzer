package edu.ncsu.mas.platys.crowdre.analyzer.util;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.filefilter.WildcardFileFilter;

public class BatchUpdater {

  public static void main(String[] args) throws FileNotFoundException, IOException,
      ClassNotFoundException, SQLException {

    String topDirName = args[0];
    if (!topDirName.endsWith("/")) {
      topDirName += "/";
    }

    Properties props = new Properties();
    try (InputStream inStream = new FileInputStream("src/main/resources/application.properties")) {
      props.load(inStream);
      Class.forName(props.getProperty("jdbc.driverClassName"));

      String updateQuery = "Update users set created_batch = ? where mturk_id = ?";
      try (Connection conn = DriverManager.getConnection(props.getProperty("jdbc.url") + "?user="
          + props.getProperty("jdbc.username") + "&password=" + props.getProperty("jdbc.password"));
          PreparedStatement updateStmt = conn.prepareStatement(updateQuery)) {

        for (int i = 1; i <= 16; i++) {
          File batchDir = new File(topDirName + "batch" + i);
          FileFilter csvFilter = new WildcardFileFilter("Batch_*.csv");
          File[] files = batchDir.listFiles(csvFilter);

          if (files.length != 1) {
            throw new IllegalStateException(
                "I except one file per batch directitory; this is not the case for " + i);
          }

          try (Reader in = new FileReader(files[0])) {
            Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);
            for (CSVRecord record : records) {
              updateStmt.setInt(1, i);
              updateStmt.setString(2, record.get("WorkerId"));
              updateStmt.addBatch();
            }
          }
        }
        updateStmt.executeBatch();
      }
    }
  }
}
