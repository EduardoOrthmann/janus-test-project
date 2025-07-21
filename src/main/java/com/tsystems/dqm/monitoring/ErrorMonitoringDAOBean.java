package com.tsystems.dqm.monitoring;

import com.tsystems.dao.xml.DAOException;
import com.tsystems.dao.xml.config.ConfigLoader;
import com.tsystems.dao.xml.db.DefaultDBAccess;
import com.tsystems.dao.xml.db.SQLStatement;
import com.tsystems.dqm.monitoring.exception.DAOColumnUniqueException;
import com.tsystems.dqm.pai.DALLoggingConstants;
import com.tsystems.util.expr.Identity;
import org.xml.sax.SAXException;

import javax.ejb.CreateException;
import javax.swing.table.DefaultTableModel;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Class ErrorMonitoringDAOBean.
 */
public class ErrorMonitoringDAOBean implements javax.ejb.SessionBean {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 3206093459760846163L;

    /** The Constant logger. */
    private static final Logger LOGGER = Logger
            .getLogger(ErrorMonitoringDAOBean.class.getName());

    /** The logging prefix. */
    private static String loggingPrefix = DALLoggingConstants.PAI_PROJ_DQMDATAACCESSEJB_ID;;

    /** The my session ctx. */
    private javax.ejb.SessionContext mySessionCtx;

    /** The config. */
    private transient ConfigLoader config = null;

    /** The Constant DB_CONFIG_PATH. */
    private static final String DB_CONFIG_PATH = "com/tsystems/dqm/monitoring";

    /** The Constant DB_CONFIG_FILE. */
    private static final String DB_CONFIG_FILE = "com/tsystems/dqm/monitoring/ErrorMonitoringManagement.xml";

    /** The Constant CONTEXT. */
    private static final String CONTEXT = "Admin";

    /** The Constant SELECT_ALL. */
    private static final String SELECT_ALL = "SELECT.ERRORCONFIG.ALL";

    /** The Constant UPDATE_ERROR_CONFIG. */
    private static final String UPDATE_ERROR_CONFIG = "UPDATE.ERRORCONFIG";

    /** The Constant INSERT_ERROR_CONFIG. */
    private static final String INSERT_ERROR_CONFIG = "INSERT.ERRORCONFIG";

    /** The Constant DELETE_ERROR_CONFIG. */
    private static final String DELETE_ERROR_CONFIG = "DELETE.ERRORCONFIG";

    /** The Constant GET_ERROR_CONFIG. */
    private static final String GET_ERROR_CONFIG = "SELECT.CONFIGS.NOTIFICATION";

    // for actual error values
    /** The Constant SELECT_ERROR_VALUE. */
    private static final String SELECT_ERROR_VALUE = "SELECT.ERRORVALUES";

    /** The Constant UPDATE_ERROR_VALUES. */
    private static final String UPDATE_ERROR_VALUES = "UPDATE.ERRORVALUES";

    /** The Constant INSERT_ERROR_VALUES. */
    private static final String INSERT_ERROR_VALUES = "INSERT.ERRORVALUES";

    /** The Constant SELECT_ACTUAL_ERROR_VALUES. */
    private static final String SELECT_ACTUAL_ERROR_VALUES = "SELECT.ACTUALERRORVALUES";

    /** The Constant GET_ACTUAL_ERRORVALUES. */
    private static final String GET_ACTUAL_ERRORVALUES = "SELECT.ERRORVALUES.NOTIFICATION";

    /** The Constant GET_HOUR_OF_LAST_RECORD. */
    private static final String GET_HOUR_OF_LAST_RECORD = "SELECT.ERRORVALUES.HOUR.OF.LAST.RECORD";

    /**
     * getSessionContext.
     *
     * @return the session context
     */
    public javax.ejb.SessionContext getSessionContext() {
        return mySessionCtx;
    }

    /**
     * setSessionContext.
     *
     * @param ctx
     * the new session context
     */
    @Override
    public void setSessionContext(javax.ejb.SessionContext ctx) {
        mySessionCtx = ctx;
    }

    /**
     * ejbCreate.
     *
     * @throws CreateException
     * the create exception
     */
    public void ejbCreate() throws javax.ejb.CreateException {
    }

    /**
     * ejbActivate.
     */
    @Override
    public void ejbActivate() {
    }

    /**
     * ejbPassivate.
     */
    @Override
    public void ejbPassivate() {
    }

    /**
     * ejbRemove.
     */
    @Override
    public void ejbRemove() {
    }

    /**
     * Initializes the configuration.
     */
    private void initConfigLoader() {
        LOGGER.log(Level.INFO, loggingPrefix + "1257" + "|"
                               + "initialize ConfigLoader");
        if (config == null) {
            config = new ConfigLoader();
            try {
                config.load(DB_CONFIG_PATH, DB_CONFIG_FILE);
            } catch (SAXException ex) {
                LOGGER.log(Level.SEVERE, () -> loggingPrefix + "1258|Could not load configuration ["
                                               + DB_CONFIG_FILE + "]: " + ex);
            }
        }
    }

    /**
     * Read error config.
     *
     * @param objectType
     * consignment or transport
     * @return the default table model
     * @throws DAOException
     * the DAO exception
     */
    public DefaultTableModel readErrorConfig(MonitoringObjectType objectType) throws DAOException {
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);
        dbAccess.setMaxNumberOfRows(0);
        Map<String, Object> p = new HashMap<>();
        addObjectTypeParam(p, objectType);
        try (Connection connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
             SQLStatement stmt = dbAccess.createPreparedStatement(SELECT_ALL, connection, p);
             ResultSet resultSet = stmt.getPreparedStatement().executeQuery()) {
            return dbAccess.resultSet2tableModel(resultSet);
        } catch (Exception exc) {
            throw new DAOException("Could not not read error configuration from database: " + exc, exc);
        }
    }

    /**
     * Gets the error config.
     *
     * @param errorStatus
     * the error status
     * @param objectType
     * consignment or transport
     * @return the error config
     * @throws DAOException
     * the DAO exception
     */
    public Map getErrorConfig(String errorStatus, MonitoringObjectType objectType) throws DAOException {
        Map<String, Object> dataMap = new HashMap<>();
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);
        dbAccess.setMaxNumberOfRows(0);
        Map<String, Object> p = new HashMap<>();
        p.put("FEHLERTYP_ID", errorStatus);
        addObjectTypeParam(p, objectType);
        try (Connection connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
             SQLStatement stmt = dbAccess.createPreparedStatement(SELECT_ALL, connection, p);
             ResultSet resultSet = stmt.getPreparedStatement().executeQuery()) {
            DefaultTableModel model = dbAccess.resultSet2tableModel(resultSet);
            for (int i = 0; i < model.getColumnCount(); i++) {
                dataMap.put(model.getColumnName(i), model.getValueAt(0, i));
            }
            return dataMap;
        } catch (Exception exc) {
            throw new DAOException("Could not not read error configuration from database: " + exc, exc);
        }
    }

    public boolean updateErrorConfig(Map _dataMap, MonitoringObjectType objectType) throws DAOException {

        // Remote EJB to Local EJB migration:
        // Create a parameter copy like remote EJB call that parameter changes doesn't
        // impact caller.
        Map dataMap = new HashMap(_dataMap);

        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);
        dbAccess.setMaxNumberOfRows(0);
        Map<String, Object> params = new HashMap<>(dataMap);
        // set Identity
        for (Object obj : params.keySet()) {
            String key = (String) obj;
            if (params.get(key) == null) {
                params.put(key, new Identity("null"));
            }
        }
        addObjectTypeParam(params, objectType);
        try (Connection connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
             SQLStatement stmt = dbAccess.createPreparedStatement(UPDATE_ERROR_CONFIG, connection, params)) {
            stmt.getPreparedStatement().executeUpdate();
            return true;
        } catch (Exception exc) {
            throw new DAOException("Could not not update error configuration in database: " + exc, exc);
        }
    }


    public boolean addErrorConfig(Map _dataMap, MonitoringObjectType objectType) throws DAOException,
            DAOColumnUniqueException {
        initConfigLoader();

        // Remote EJB to Local EJB migration:
        // Create a parameter copy like remote EJB call that parameter changes doesn't
        // impact caller.
        Map dataMap = new HashMap(_dataMap);

        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);
        dbAccess.setMaxNumberOfRows(0);
        // set Identity
        for (Object obj : dataMap.keySet()) {
            String key = (String) obj;
            if (dataMap.get(key) == null) {
                dataMap.put(key, new Identity("null"));
            }
        }
        addObjectTypeParam(dataMap, objectType);
        try (Connection connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
             SQLStatement stmt = dbAccess.createPreparedStatement(INSERT_ERROR_CONFIG, connection, dataMap)) {
            stmt.getPreparedStatement().execute();
            return true;
        } catch (Exception exc) {
            if (exc.getMessage().contains("SQLCODE=-803")) {
                throw new DAOColumnUniqueException(exc.toString(), exc);
            }
            throw new DAOException("Could not add error configuration in database: " + exc, exc);
        }
    }

    /**
     * Delete error config.
     *
     * @param errorsStatus
     * the errors status
     * @param objectType
     * consignment or transport
     * @return true, if successful
     * @throws DAOException
     * the DAO exception
     */
    public boolean deleteErrorConfig(String errorsStatus, MonitoringObjectType objectType) throws DAOException {
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);
        dbAccess.setMaxNumberOfRows(0);
        Map<String, Object> p = new HashMap<>();
        p.put("FEHLERTYP_ID", errorsStatus);
        addObjectTypeParam(p, objectType);
        try (Connection connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
             SQLStatement stmt = dbAccess.createPreparedStatement(DELETE_ERROR_CONFIG, connection, p)) {
            stmt.getPreparedStatement().executeUpdate();
            return true;
        } catch (Exception exc) {
            throw new DAOException("Could not delete error configuration from database: " + exc, exc);
        }
    }

    /**
     * Check duplicate.
     *
     * @param errorsStatus
     * the errors status
     * @param objectType
     * consignment or transport
     * @return true if the error configuration is already in database
     * @throws DAOException
     * the DAO exception
     */
    public boolean checkDuplicate(String errorsStatus, MonitoringObjectType objectType) throws DAOException {
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);
        dbAccess.setMaxNumberOfRows(0);
        Map<String, Object> p = new HashMap<>();
        p.put("FEHLERTYP_ID", errorsStatus);
        addObjectTypeParam(p, objectType);
        try (Connection connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
             SQLStatement stmt = dbAccess.createPreparedStatement(SELECT_ALL, connection, p);
             ResultSet resultSet = stmt.getPreparedStatement().executeQuery()) {
            if (resultSet.next()) {
                return true;
            } else {
                return false;
            }
        } catch (Exception exc) {
            throw new DAOException("Could not check duplicates in database: " + exc, exc);
        }
    }

    /**
     * Check if exists.
     *
     * @param errorsConfig
     * the errors config
     * @param objectType
     * consignment or transport
     * @return the integer
     * @throws DAOException
     * the DAO exception
     */
    public Integer checkIfExists(String errorsConfig, MonitoringObjectType objectType) throws DAOException {
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);
        dbAccess.setMaxNumberOfRows(0);
        Map<String, Object> p = new HashMap<>();
        p.put("FEHLERTYP", errorsConfig);
        addObjectTypeParam(p, objectType);
        try (Connection connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
             SQLStatement stmt = dbAccess.createPreparedStatement(SELECT_ERROR_VALUE, connection, p);
             ResultSet resultSet = stmt.getPreparedStatement().executeQuery()) {
            if (resultSet.next()) {
                return resultSet.getInt("ANZAHL");
            } else {
                return null;
            }
        } catch (Exception exc) {
            LOGGER.log(Level.SEVERE, loggingPrefix + "1260" + "|"
                                     + "Could not read error configuration from database", exc);
            throw new DAOException("Could not check existence in database: " +exc, exc);
        }
    }

    public boolean addErrorValue(Map _dataMap, MonitoringObjectType objectType) throws DAOException {
        initConfigLoader();

        // Remote EJB to Local EJB migration:
        // Create a parameter copy like remote EJB call that parameter changes doesn't
        // impact caller.
        Map dataMap = new HashMap(_dataMap);

        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);
        dbAccess.setMaxNumberOfRows(0);
        addObjectTypeParam(dataMap, objectType);
        try (Connection connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
             SQLStatement stmt = dbAccess.createPreparedStatement(INSERT_ERROR_VALUES, connection, dataMap)) {
            stmt.getPreparedStatement().execute();
            return true;
        } catch (Exception exc) {
            throw new DAOException("Could not not add error value in database: " + exc, exc);
        }
    }

    public boolean updateErrorValue(Map _dataMap, MonitoringObjectType objectType) throws DAOException {

        // Remote EJB to Local EJB migration:
        // Create a parameter copy like remote EJB call that parameter changes doesn't
        // impact caller.
        Map dataMap = new HashMap(_dataMap);

        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);
        dbAccess.setMaxNumberOfRows(0);
        addObjectTypeParam(dataMap, objectType);
        try (Connection connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
             SQLStatement stmt = dbAccess.createPreparedStatement(UPDATE_ERROR_VALUES, connection, dataMap)) {
            stmt.getPreparedStatement().executeUpdate();
            return true;
        } catch (Exception exc) {
            throw new DAOException("Could not not update error value in database: " + exc, exc);
        }
    }

    /**
     * Gets the actual error values.
     *
     * @param date
     * the date
     * @param objectType
     * consignment or transport
     * @return the actual error values
     * @throws DAOException
     * the DAO exception
     */
    public DefaultTableModel getActualErrorValues(String date, MonitoringObjectType objectType)
            throws DAOException {
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);
        dbAccess.setMaxNumberOfRows(0);
        Map<String, Object> p = new HashMap<>();
        p.put("DATE", date);
        addObjectTypeParam(p, objectType);
        try (Connection connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
             SQLStatement stmt = dbAccess.createPreparedStatement(SELECT_ACTUAL_ERROR_VALUES, connection, p);
             ResultSet resultSet = stmt.getPreparedStatement().executeQuery()) {
            return dbAccess.resultSet2tableModel(resultSet);
        } catch (Exception exc) {
            throw new DAOException("Could not not read error values from database: " + exc, exc);
        }
    }

    /**
     * Gets the values.
     *
     * @param start
     * the start
     * @param end
     * the end
     * @param objectType
     * consignment or transport
     * @return the values
     * @throws DAOException
     * the DAO exception
     */
    public Map<String, Integer> getValues(Timestamp start, Timestamp end, MonitoringObjectType objectType)
            throws DAOException {
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);
        dbAccess.setMaxNumberOfRows(0);
        Map<String, Object> params = new HashMap<>();
        params.put("FROM", start.toString());
        params.put("TO", end.toString());
        addObjectTypeParam(params, objectType);
        Map<String, Integer> actualValues = new HashMap<>();
        try (Connection connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
             SQLStatement stmt = dbAccess.createPreparedStatement(GET_ACTUAL_ERRORVALUES, connection, params);
             ResultSet resultSet = stmt.getPreparedStatement().executeQuery()) {
            DefaultTableModel model = dbAccess.resultSet2tableModel(resultSet);
            for (int i = 0; i < model.getRowCount(); i++) {
                String errorKey = (String) model.getValueAt(i, 0);
                if (actualValues.containsKey(errorKey)) {
                    actualValues.put(errorKey, actualValues.get(errorKey)
                                               + (Integer) model.getValueAt(i, 1));
                } else {
                    actualValues
                            .put(errorKey, (Integer) model.getValueAt(i, 1));
                }
            }
            return actualValues;
        } catch (Exception exc) {
            throw new DAOException("Could not not read error values of current hour from database: " + exc, exc);
        }
    }

    /**
     * Gets the configs.
     *
     * @param timeIntervalFrom
     * the time interval from
     * @param timeIntervalTo
     * the time interval to
     * @param objectType
     * consignment or transport
     * @return the configs
     * @throws DAOException
     * the DAO exception
     */
    public Map<String, ErrorConfigHolder> getConfigs(String timeIntervalFrom,
                                                     String timeIntervalTo, MonitoringObjectType objectType) throws DAOException {
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);
        dbAccess.setMaxNumberOfRows(0);
        Map<String, Object> params = new HashMap<>();
        params.put("ZIELWERT_FROM", new Identity(timeIntervalFrom));
        params.put("ZIELWERT_TO", new Identity(timeIntervalTo));
        addObjectTypeParam(params, objectType);
        Map<String, ErrorConfigHolder> intervalConfig = new HashMap<String, ErrorConfigHolder>();
        try (Connection connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
             SQLStatement stmt = dbAccess.createPreparedStatement(GET_ERROR_CONFIG, connection, params);
             ResultSet resultSet = stmt.getPreparedStatement().executeQuery()) {
            DefaultTableModel model = dbAccess.resultSet2tableModel(resultSet);
            for (int i = 0; i < model.getRowCount(); i++) {
                ErrorConfigHolder iConfig = new ErrorConfigHolder();
                iConfig.setDevAbove((Integer) model.getValueAt(i, 1));
                iConfig.setDevBelow((Integer) model.getValueAt(i, 2));
                iConfig.setTargetFrom((Integer) model.getValueAt(i, 3));
                iConfig.setTargetTo((Integer) model.getValueAt(i, 4));
                intervalConfig.put((String) model.getValueAt(i, 0), iConfig);
            }
            return intervalConfig;
        } catch (SQLException exc) {
            throw new DAOException("Could not not read error configuration from database: " + exc, exc);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks if is time to send mail.
     *
     * @param objectType
     * consignment or transport
     * @return true, if is time to send mail
     * @throws DAOException
     * the DAO exception
     */
    public boolean isTimeToSendMail(MonitoringObjectType objectType) throws DAOException {
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);
        dbAccess.setMaxNumberOfRows(0);
        Map<String, Object> p = new HashMap<>();
        addObjectTypeParam(p, objectType);
        try (Connection connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
             SQLStatement stmt = dbAccess.createPreparedStatement(GET_HOUR_OF_LAST_RECORD, connection, p);
             ResultSet resultSet = stmt.getPreparedStatement().executeQuery()) {
            DefaultTableModel model = dbAccess.resultSet2tableModel(resultSet);
            Integer flag = (Integer) model.getValueAt(0, 0);
            if (flag.equals(1)) {
                return true;
            }
            return false;
        } catch (Exception exc) {
            throw new DAOException("Could not get date of last record from actual error values table: " + exc, exc);
        }
    }

    /**
     * Add monitoring object type to given SQL parameters.
     *
     * @param parameters
     * parameters (will be changed)
     * @param objectType
     * consignment or transport
     */
    private void addObjectTypeParam(Map parameters, MonitoringObjectType objectType) {
        parameters.put("DATENTYP", objectType.getType());
    }

}