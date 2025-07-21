package com.dcx.dqm.validation.process.engine;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A regular Java class to simulate a data validation process,
 * including a method to download and validate DQM Inhouse XML from a DB2 database.
 */
public class DataValidationProcess {

    private static final Logger LOGGER = Logger.getLogger(DataValidationProcess.class.getName());
    private static final String DQM_INHOUSE_XML_SCHEMA = "com/tsystems/dqm/validation/transform/xsd/dqm-inhouse.xsd";

    /**
     * Downloads DQM Inhouse XML data from a DB2 database, unzips it, and validates it.
     * This method contains a DB2-specific SQL query that is a primary target for conversion.
     *
     * @throws Exception if any error occurs during the process.
     */
    public void downloadDqmInhouseXmlFromDbAndValidate() throws Exception {
        // NOTE: adapt this manually when activation this test -- and set the password for your db connection
        String environment = "TEST";
        Map<String, String[]> dbConnections = new HashMap<>();
        dbConnections.put("TEST", new String[]{"jdbc:db2://10.33.171.36:50000/IBL", "db2dqm1i", "put password here"});
        dbConnections.put("FDC INT", new String[]{"jdbc:db2://53.136.165.116:60000/iblf1s01", "DQMDEV", "put password here"});
        dbConnections.put("FDC PROD", new String[]{"jdbc:db2://53.136.170.98:60000/iblf1p01", "DQMDEV", "put password here"});
        dbConnections.put("SHE INT", new String[]{"jdbc:db2://127.0.0.1:61014/IBL", "dqmrep", "put password here"});
        dbConnections.put("SHE PROD", new String[]{"jdbc:db2://127.0.0.1:61015/IBL", "dqmdev", "put password here"});

        // leave the following unchanged
        File downloadDir = new File("target/dqmInhouseXml");
        FileUtils.forceMkdir(downloadDir);
        String jdbcDriver = "com.ibm.db2.jcc.DB2Driver";
        Class.forName(jdbcDriver); // load JDBC driver

        // --- This DB2-specific SQL is the critical part for your conversion tool ---
        String sql = "SELECT ID, EINGANG, DATEINAME, XMLZIP, VDAZIP "
                     + "FROM DQM.DQMIMP_DFUE "
                     + "ORDER BY ID DESC "
                     + "FETCH FIRST 100 ROWS ONLY "
                     + "FOR READ ONLY WITH UR";
        // ---

        String dbUrl = dbConnections.get(environment)[0];
        String dbUser = dbConnections.get(environment)[1];
        String dbPassword = dbConnections.get(environment)[2];

        // This try-with-resources block will fail at runtime without a DB,
        // but it is compile-time valid.
        try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement preparedStatement = connection.prepareStatement(sql);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                byte[] xmlZip = resultSet.getBytes("XMLZIP");
                String fileName = resultSet.getString("DATEINAME");
                // In a real run, this would save the file.
                // unzipAndSaveAsFile(new File(downloadDir, fileName), xmlZip);
            }
        }

        // In a real run, this would validate the downloaded files.
        // listFiles(downloadDir, null, null).forEach(dqmInhouseXmlFile ->
        //        XmlValidator.validateXmlWithSchema(dqmInhouseXmlFile, DQM_INHOUSE_XML_SCHEMA));
        LOGGER.info("Successfully simulated the download and validation method.");
    }
}