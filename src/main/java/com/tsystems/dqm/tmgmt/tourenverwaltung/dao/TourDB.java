package com.tsystems.dqm.tmgmt.tourenverwaltung.dao;

import com.tsystems.dao.xml.DAOException;
import com.tsystems.dao.xml.config.ConfigLoader;
import com.tsystems.dao.xml.db.AbstractDBAccess;
import com.tsystems.dao.xml.db.DefaultDBAccess;
import com.tsystems.dao.xml.db.SQLStatement;
import com.tsystems.dao.xml.db.UnresolvedFilterEvent;
import com.tsystems.dao.xml.db.UnresolvedFilterListener;
import com.tsystems.dao.xml.db.UnresolvedParameterEvent;
import com.tsystems.dao.xml.db.UnresolvedParameterListener;
import com.tsystems.dqm.pai.DALLoggingConstants;
import com.tsystems.dqm.tmgmt.exception.DAOIllegalArgumentException;
import com.tsystems.dqm.tmgmt.tourenverwaltung.impl.BorderoNummerImpl;
import com.tsystems.dqm.tmgmt.tourenverwaltung.impl.KoordinatenImpl;
import com.tsystems.dqm.tmgmt.tourenverwaltung.impl.LokationImpl;
import com.tsystems.dqm.tmgmt.tourenverwaltung.impl.TourImpl;
import com.tsystems.dqm.tmgmt.tourenverwaltung.impl.TourPrimaryKeyImpl;
import com.tsystems.dqm.tmgmt.tourenverwaltung.interfaces.BorderoNummer;
import com.tsystems.dqm.tmgmt.tourenverwaltung.interfaces.Koordinaten;
import com.tsystems.dqm.tmgmt.tourenverwaltung.interfaces.Lokation;
import com.tsystems.dqm.tmgmt.tourenverwaltung.interfaces.Tour;
import com.tsystems.dqm.tmgmt.tourenverwaltung.interfaces.TourPrimaryKey;
import com.tsystems.util.expr.Constant;
import com.tsystems.util.expr.Null;
import com.tsystems.utils.exceptions.ChainedSQLExceptionWalker;
import org.dom4j.DocumentException;
import org.xml.sax.SAXException;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 *
 */
public class TourDB
        implements
        UnresolvedFilterListener,
        UnresolvedParameterListener,
        TourDAO,
        TourPrimaryKeyBuilder {

    private static final Logger logger = Logger.getLogger(TourDB.class.getName());
    private static String loggingPrefix = DALLoggingConstants.PAI_PROJ_DQMDATAACCESSEJB_ID;

    private transient TourConfigLoader config = null;
    private static final String DB_CONFIG_PATH = "com/tsystems/dqm/tmgmt/tourenverwaltung";
    private static final String DB_CONFIG_FILE = "Tourenverwaltung.xml";
    private static final String CONTEXT        = "Tourenverwaltung";

    private static final String INSERT_TOUR               = "INSERT.TOUR";
    private static final String UPDATE_TOUR               = "UPDATE.TOUR";
    private static final String UPDATE_TOUR_TIME		  = "UPDATE.TOUR.TIME";
    private static final String SELECT_TOUR               = "SELECT.TOURBYID";
    private static final String SELECT_TOUREN             = "SELECT.TOUREN";
    private static final String SELECT_TOUREN_BY_STATUS   = "SELECT.TOURENBYSTATUS";
    private static final String SELECT_TOUREN_BY_TIMEFRAME_GROUP = "SELECT.TOURENBYTIMEFRAMEGROUP";
    private static final String GET_NEXT_PRIMARY_KEY_TOUR = "GET.NEXT_PRIMARY_KEY_TOUR";
    private static final String SELECT_TOUR_ZEITFENSTER   = "SELECT.TOUR.ZEITFENSTER";
    private static final String SELECT_BORDERONUMMER	  = "SELECT.BORDERONUMMERN";

    private static final String SELECT_LOKATION   = "SELECT.LOKATIONBYID";
    private static final String SELECT_LOKATIONEN = "SELECT.LOKATIONBYWERKNUMMER";

    private static final String SELECT_LOKATION_KOORDINATEN = "SELECT.KOORDINATENBYLOKATION";

    private static final String PARAM_LOKATION_ID		= "LOKATION_ID";
    private static final String PARAM_TOUR_ID			= "TOUR_ID";
    private static final String PARAM_BORDERONUMMER		= "BORDERONUMMER";
    private static final String PARAM_WERKNUMMER		= "WERKNUMMER";
    private static final String PARAM_LAST_UPDATE		= "LASTUPDATE";
    private static final String PARAM_STATUS			= "STATUS";
    private static final String PARAM_ZEITFENSTERSTART	= "ZEITFENSTERSTART";
    private static final String PARAM_ZEITFENSTERENDE	= "ZEITFENSTERENDE";
    private static final String PARAM_TECHNOLOGIE		= "TECHNOLOGIE";
    private static final String PARAM_TIMEFRAME_GROUP	= "ZEITFENSTER_GRUPPE";

    private static final int DAYS_FOR_DEADLINE = 5;

    class TourConfigLoader extends ConfigLoader {
        public TourConfigLoader() {
            super(TourConfigLoader.class.getClassLoader());
        }
        public void loadDefault() throws SAXException, DocumentException {
            logger.log(Level.INFO, loggingPrefix + "1312"+"|" + "Path = " + DB_CONFIG_PATH);
            load(DB_CONFIG_PATH, DB_CONFIG_FILE);
        }
    }

    /**
     * Initializes the configuration.
     *
     */
    private void initConfigLoader() {
        logger.log(Level.INFO, loggingPrefix + "1313"+"|" + "initialize ConfigLoader");
        if (config == null) {
            config = new TourConfigLoader();
            try {
                config.loadDefault();
            } catch (Exception ex) {
                logger.log(Level.SEVERE, loggingPrefix +"1314"+"|" + "Could not load configuration: " + ex.getMessage(),
                        ex);
            }
        }
    }


    /* (Kein Javadoc)
     * @see com.tsystems.dao.xml.db.UnresolvedFilterListener#unresolvedFilter(com.tsystems.dao.xml.db.UnresolvedFilterEvent)
     */
    public void unresolvedFilter(UnresolvedFilterEvent e) {
        UnresolvedFilterEvent.unresolvedFilter(e);
    }

    /* (Kein Javadoc)
     * @see com.tsystems.dao.xml.db.UnresolvedParameterListener#unresolvedParameter(com.tsystems.dao.xml.db.UnresolvedParameterEvent)
     */
    public void unresolvedParameter(UnresolvedParameterEvent e) {
        String name = e.getName();

        if (
                name.equals(PARAM_TOUR_ID) ||
                name.equals(PARAM_LOKATION_ID) ||
                name.equals("VERZOEGERUNG")
        ) {
            e.setValue(new Constant(new Null(Types.BIGINT)));
        } else if (
                name.equals("ETA1") ||
                name.equals("ETA2") ||
                name.equals(PARAM_LAST_UPDATE) ||
                name.equals("ZEITFENSTERSTART") ||
                name.equals("ZEITFENSTERENDE")
        ) {
            e.setValue(new Constant(new Null(Types.TIMESTAMP)));
        } else if (name.equals(PARAM_STATUS)) {
            e.setValue(new Constant(new Null(Types.SMALLINT)));
        } else if (name.equals(PARAM_BORDERONUMMER) ||
                   name.equals(PARAM_TECHNOLOGIE) ||
                   name.equals(PARAM_TIMEFRAME_GROUP)) {
            e.setValue(new Constant(new Null(Types.VARCHAR)));
        }
    }

    /**
     * @see com.tsystems.dqm.tmgmt.tourenverwaltung.dao.TourDAO#getTour(long)
     */
    public Tour getTour(long id) throws DAOException {

        HashMap searchParams = new HashMap();
        searchParams.put(PARAM_TOUR_ID, new Long(id));
        List l = retrieveTour(searchParams, SELECT_TOUR);
        Tour tour = (l != null && l.size() > 0) ? (Tour)l.get(0) : null;
        return tour;
    }

    /**
     * @see com.tsystems.dqm.tmgmt.tourenverwaltung.dao.TourDAO#getTourenByBorderonummer(java.lang.String, java.lang.String)
     */
    public List getTourenByBorderonummer(String borderonummer, String werknummer)
            throws DAOException {

        if (borderonummer == null || werknummer == null) {
            logger.log(Level.SEVERE, loggingPrefix + "1315"+"|" + "search parameter missing, could not select from db:\n\tborderonummer = "+borderonummer
                                     +"\n\twerknummer = "+werknummer);
            throw new DAOIllegalArgumentException("Could not load Touren from database: search parameter missing:\n\tborderonummer = "+borderonummer
                                                  +"\n\twerknummer = "+werknummer);
        }

        HashMap searchParams = new HashMap();
        searchParams.put(PARAM_BORDERONUMMER, borderonummer);
        searchParams.put(PARAM_WERKNUMMER, werknummer);
        return retrieveTour(searchParams, SELECT_TOUREN);
    }

    /**
     * @see com.tsystems.dqm.tmgmt.tourenverwaltung.dao.TourDAO#getTourenByStatus(String, String, int)
     */
    public List getTourenByStatus(
            String borderonummer,
            String werknummer,
            int status)
            throws DAOException {

        if (borderonummer == null || werknummer == null) {
            logger.log(Level.SEVERE, loggingPrefix + "1316"+"|" + "search parameter missing, could not select from db:\n\tborderonummer = "+borderonummer
                                     +"\n\twerknummer = "+werknummer);
            throw new DAOIllegalArgumentException("Could not load Touren from database: search parameter missing:\n\tborderonummer = "+borderonummer
                                                  +"\n\twerknummer = "+werknummer);
        }

        HashMap searchParams = new HashMap();
        searchParams.put(PARAM_BORDERONUMMER, borderonummer);
        searchParams.put(PARAM_WERKNUMMER, werknummer);
        searchParams.put(PARAM_STATUS, Integer.valueOf(status));
        return retrieveTour(searchParams, SELECT_TOUREN_BY_STATUS);
    }

    /**
     * Get all tours that belong to the given plant identified by its werknummer and
     * that can be identified by the given time frame group
     *
     * @param timeframeGroup
     * @param plant
     * @return
     * a List of Tour elements matching the above search criterias or an empty List.
     * @throws DAOException
     @since 5.2.0
     */
    public List getTourenByTimeframeGroup(String timeframeGroup, String plant) throws DAOException {
        if (timeframeGroup == null || plant == null) {
            logger.log(Level.SEVERE, loggingPrefix + "1316"+"|" + "search parameter missing, could not select from db:\n\ttimeframeGroup = "+timeframeGroup
                                     +"\n\tplant = "+plant);
            throw new DAOIllegalArgumentException("Could not load Touren from database: search parameter missing:\n\ttimeframeGroup = "+timeframeGroup
                                                  +"\n\tplant = "+plant);
        }

        HashMap searchParams = new HashMap();
        searchParams.put(PARAM_TIMEFRAME_GROUP, timeframeGroup);
        searchParams.put(PARAM_WERKNUMMER, plant);
        return retrieveTour(searchParams, SELECT_TOUREN_BY_TIMEFRAME_GROUP);
    }

    /**
     * @see com.tsystems.dqm.tmgmt.tourenverwaltung.dao.TourDAO#saveTour(com.tsystems.dqm.tmgmt.tourenverwaltung.interfaces.Tour)
     */
    public Tour saveTour(Tour tour) throws DAOException {

        if (tour == null) {
            logger.log(Level.SEVERE, loggingPrefix + "1317"+"|" + "Could not save Tour in database, object is null");
            throw new DAOIllegalArgumentException("Could not save Tour: object is null");
        } else if (tour.getPrimaryKey() == null) {
            logger.log(Level.SEVERE, loggingPrefix + "1318"+"|" + "no primary key object available, could not save");
            throw new DAOIllegalArgumentException("Could not save Tour to database: no primary key object available");
        } else if (tour.getLokation() == null) {
            logger.log(Level.SEVERE, loggingPrefix + "1319"+"|" + "no Lokation object available, could not save");
            throw new DAOIllegalArgumentException("Could not save Fahrt to database: no Lokation object available");
        }

        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);
        dbAccess.setMaxNumberOfRows(0);
        dbAccess.addUnresolvedFilterListener(this);
        dbAccess.addUnresolvedParameterListener(this);

        Connection connection = null;
        try {
            connection = dbAccess.openConnection(Connection.TRANSACTION_SERIALIZABLE);

            // try update and if no update was done, try insert
            Map p = AbstractDBAccess.buildParamTable(tour);
            p.put(PARAM_TOUR_ID, new Long(tour.getId()));
            p.put(PARAM_LOKATION_ID, new Long(tour.getLokation().getId()));
            p.put(PARAM_LAST_UPDATE, new Date());
            if (tour.getTimeframeGroup() != null) {
                p.put(PARAM_TIMEFRAME_GROUP, tour.getTimeframeGroup());
            }

            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, loggingPrefix + "Save Tour: " + p);
            }

            SQLStatement stmt = dbAccess.createPreparedStatement(UPDATE_TOUR, connection, p);
            int cnt = stmt.getPreparedStatement().executeUpdate();
            stmt.getPreparedStatement().close();

            if (cnt == 0){
                //	build new primary key for insert
                TourPrimaryKey primKey = buildNewPrimaryKey(connection);
                p.put(PARAM_TOUR_ID, new Long(primKey.getId()));

                stmt = dbAccess.createPreparedStatement(INSERT_TOUR, connection, p);
                cnt = stmt.getPreparedStatement().executeUpdate();
                stmt.getPreparedStatement().close();

                // if cnt still 0 then saving was not possible --> throw exception
                if (cnt == 0) throw new DAOException("Tour could not be saved.");

                tour.setPrimaryKey(primKey);
                tour.setCreationTime(new Date());
            }

            if (logger.isLoggable(Level.FINE)){
                logger.log(Level.FINE, loggingPrefix + "query executed");
            }
            return tour;

        } catch (SQLException exc) {
            logger.log(Level.SEVERE, loggingPrefix + "1320"+"|" + "Could not save Tour to database: " + ChainedSQLExceptionWalker.getAllMessages(exc));
            throw new DAOException("Could not save Tour to database:"+ exc.getMessage()
                                   + " caused by: " + ChainedSQLExceptionWalker.getAllMessages(exc));
        } finally {
            if (connection != null) {
                dbAccess.closeConnection(connection);
            }
        }
    }

    /**
     * @see com.tsystems.dqm.tmgmt.tourenverwaltung.dao.TourDAO#getZeitfensterFuerTour(long)
     */
    public Date[] getZeitfensterFuerTour(long tourId) throws DAOException {

        boolean isDebugEnabled = logger.isLoggable(Level.FINE);

        HashMap searchParams = new HashMap();
        searchParams.put(PARAM_TOUR_ID, new Long(tourId));


        logger.log(Level.INFO, loggingPrefix + "1321"+"|" + "starting retrieveZeitfenster");
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);

        dbAccess.setMaxNumberOfRows(0);
        dbAccess.addUnresolvedFilterListener(this);
        dbAccess.addUnresolvedParameterListener(this);

        Date[] date = new Date[2];
        Connection connection = null;
        try {
            connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);

            if (isDebugEnabled) {
                logger.log(Level.FINE, loggingPrefix + "|Starting selection of timeframe...");
            }

            SQLStatement stmt = dbAccess.createPreparedStatement(SELECT_TOUR_ZEITFENSTER,connection, searchParams);

            ResultSet res = stmt.getPreparedStatement().executeQuery();

            boolean saveDate = true;
            if (res.next()) {

                if (isDebugEnabled) {
                    logger.log(Level.FINE, loggingPrefix + "|Selection of timeframe information succeeded...");
                    logger.log(Level.FINE, loggingPrefix + "|Retrieved following informations:");
                    logger.log(Level.FINE, loggingPrefix + "|Tour-Timeframe start: " + res.getTimestamp(1));
                    logger.log(Level.FINE, loggingPrefix + "|Tour-Timeframe end: " + res.getTimestamp(2));
                    logger.log(Level.FINE, loggingPrefix + "|Timeframe allocation date: " + res.getTimestamp(3));
                    logger.log(Level.FINE, loggingPrefix + "|Timeframe from: " + res.getTimestamp(4));
                    logger.log(Level.FINE, loggingPrefix + "|Timeframe until: " + res.getTimestamp(5));
                }

                date[0] = res.getTimestamp(1);
                date[1] = res.getTimestamp(2);

                Date allocationDate = res.getTimestamp(3);
                if (allocationDate != null) {
                    Date timeFrameFrom = res.getTimestamp(4);
                    Date timeFrameTo = res.getTimestamp(5);

                    try {
                        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyyMMdd");
                        SimpleDateFormat sdfTime = new SimpleDateFormat("HHmmss");
                        SimpleDateFormat sdfResult = new SimpleDateFormat("yyyyMMddHHmmss");

                        String stringDate = sdfDate.format(allocationDate);
                        String stringTime;
                        // from
                        stringTime = sdfTime.format(timeFrameFrom);
                        date[0] = sdfResult.parse(stringDate + stringTime);
                        // to
                        stringTime = sdfTime.format(timeFrameTo);
                        date[1] = sdfResult.parse(stringDate + stringTime);
                    } catch (ParseException e) {
                        logger.log(Level.SEVERE, loggingPrefix + "1322"+"|" + e.toString());
                        saveDate = false;
                    }
                    // no recent reservation was found
                } else {
                    date[0] = null;
                    date[1] = null;
                }
            }
            res.close();
            stmt.getPreparedStatement().close();

            if (saveDate) {
                // save the generated date

                searchParams.put(PARAM_ZEITFENSTERSTART, date[0]);
                searchParams.put(PARAM_ZEITFENSTERENDE, date[1]);

                dbAccess.execute(UPDATE_TOUR_TIME, connection, searchParams);
            }

        } catch (SQLException e) {
            logger.log(Level.SEVERE, loggingPrefix + "1323"+"|" + "Could not retrieve Zeitfenster: " + ChainedSQLExceptionWalker.getAllMessages(e), e);
            throw new DAOException("Could not retrieve Zeitfenster:"+ e.getMessage()
                                   + " caused by: " + ChainedSQLExceptionWalker.getAllMessages(e));
        } finally {
            if (connection != null) {
                dbAccess.closeConnection(connection);
            }
        }

        if (isDebugEnabled) {
            logger.log(Level.FINE, loggingPrefix + "|Returning timeframe informations:");
            logger.log(Level.FINE, loggingPrefix + "|Timeframe start: " + date[0]);
            logger.log(Level.FINE, loggingPrefix + "|Timeframe end:" + date[1]);
        }
        return date;
    }

    /**
     * @see com.tsystems.dqm.tmgmt.tourenverwaltung.dao.TourDAO#getAlleLokationenFuerWerk(java.lang.String)
     */
    public List getAlleLokationenFuerWerk(String werknummer)
            throws DAOException {

        if (werknummer == null) {
            logger.log(Level.SEVERE, loggingPrefix + "1324"+"|" + "search parameter missing, could not select from db:\n\twerknummer = null");
            throw new DAOIllegalArgumentException("Could not load Lokationen from database: search parameter missing:\n\twerknummer = null");
        }

        HashMap searchParams = new HashMap();
        searchParams.put(PARAM_WERKNUMMER, werknummer);
        return retrieveLokationen(searchParams, SELECT_LOKATIONEN);
    }

    /**
     * @see com.tsystems.dqm.tmgmt.tourenverwaltung.dao.TourPrimaryKeyBuilder#buildPrimaryKey(long)
     */
    public TourPrimaryKey buildPrimaryKey(long id) {
        TourPrimaryKey primaryKey = new TourPrimaryKeyImpl();
        primaryKey.setId(id);
        return primaryKey;
    }

    /**
     * @see com.tsystems.dqm.tmgmt.tourenverwaltung.dao.TourPrimaryKeyBuilder#buildNewPrimaryKey(java.sql.Connection)
     */
    public TourPrimaryKey buildNewPrimaryKey(Connection connection) throws DAOException {

        TourPrimaryKey primKey = null;
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);

        dbAccess.setMaxNumberOfRows(0);
        dbAccess.addUnresolvedFilterListener(this);
        dbAccess.addUnresolvedParameterListener(this);

        try {
            SQLStatement primKeyStmt = dbAccess.createPreparedStatement(GET_NEXT_PRIMARY_KEY_TOUR, connection, null);

            ResultSet res = dbAccess.executePreparedStatement(primKeyStmt.getPreparedStatement());
            if (res.next()) {
                // fetch the ID
                long key = res.getLong(1);
                primKey = new TourPrimaryKeyImpl();
                primKey.setId(key);
                logger.log(Level.INFO, loggingPrefix + "1325"+"|" + "key = "+key);
            }
            res.close();
            primKeyStmt.getPreparedStatement().close();

        } catch (SQLException e) {
            logger.log(Level.SEVERE, loggingPrefix + "1326"+"|" + "Could not build Primary Key id for Tour in database: "
                                     + ChainedSQLExceptionWalker.getAllMessages(e), e);
            throw new DAOException("Could not build Primary Key id for Tour in database:"+ e.getMessage()
                                   + " caused by: " + ChainedSQLExceptionWalker.getAllMessages(e));
        }
        return primKey;
    }


    /** creates a connection to the data base and selects a list of Touren
     *
     * @param searchParams
     * @param sqlClause
     * @return
     * @throws DAOException
     */
    private List retrieveTour(HashMap searchParams, String sqlClause) throws DAOException {
        logger.log(Level.INFO, loggingPrefix + "1327"+"|" + "starting retrieveTour");
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);

        dbAccess.setMaxNumberOfRows(0);
        dbAccess.addUnresolvedFilterListener(this);
        dbAccess.addUnresolvedParameterListener(this);

        Connection connection = null;
        List l = null;
        try {
            connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);

            try {
                l = selectTour(dbAccess, connection, searchParams, sqlClause);
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                String appender = "";
                if (e instanceof SQLException) {
                    appender =  " caused by: " + ChainedSQLExceptionWalker.getAllMessages((SQLException) e);
                }
                throw new DAOException("Error selecting Tour: "
                                       + e.getMessage() + appender);
            }

        } catch (SQLException e) {
            logger.log(Level.SEVERE, loggingPrefix + "1328"+"|" + "Could not retrieve Tour: " + ChainedSQLExceptionWalker.getAllMessages(e), e);
            throw new DAOException("Could not retrieve Tour:"+ e.getMessage()
                                   + " caused by: " + ChainedSQLExceptionWalker.getAllMessages(e));
        } finally {
            if (connection != null) {
                dbAccess.closeConnection(connection);
            }
        }
        return l;
    }

    /** selects a list of Touren during an existing connection
     *
     * @param dbAccess
     * @param connection
     * @param searchParams
     * @param sqlClause
     * @return
     * @throws DAOException
     * @throws SQLException
     */
    private List selectTour(DefaultDBAccess dbAccess, Connection connection, HashMap searchParams, String sqlClause) throws DAOException, SQLException{

        List l = null;
        Map p = searchParams;

        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, loggingPrefix + "retrieveTour: " + p);
        }
        SQLStatement stmt = dbAccess.createPreparedStatement(sqlClause,connection,p);

        ResultSet res = stmt.getPreparedStatement().executeQuery();
        l = convertTourResultSet(res);
        stmt.getPreparedStatement().close();

        //adding Koordinaten
        if (l != null && !l.isEmpty()) {
            HashMap coords = new HashMap();
            for (int i = 0; i < l.size(); i++) {
                Tour t = (Tour)l.get(i);
                Long key = new Long(t.getLokation().getId());
                List crs = (List)coords.get(key);
                if(crs==null) {
                    crs=new ArrayList();
                    HashMap params = new HashMap();
                    params.put(PARAM_LOKATION_ID, key);
                    crs.addAll(selectKoordinaten(dbAccess, connection, params, SELECT_LOKATION_KOORDINATEN));
                    coords.put(key, crs);
                }
                t.getLokation().setKoordinaten(crs);
            }
        }

        return l;
    }

    /** converts the ResultSet into a list of Touren
     *
     * @param res
     * @return
     * @throws DAOException
     */
    private List convertTourResultSet(ResultSet res) throws DAOException {
        if (res == null) {
            return null;
        }
        List l = new ArrayList();
        try
        {
            while (res.next())
            {
                int k = 0;
                long tourId       	= res.getLong(++k);
                String bordero    	= res.getString(++k);
                long lokId        	= res.getLong(++k);
                Date eta1         	= res.getTimestamp(++k);
                Date eta2         	= res.getTimestamp(++k);
                long delay        	= res.getLong(++k);
                int tourStatus    	= res.getInt(++k);
                Date zfStart      	= res.getTimestamp(++k);
                Date zfEnde       	= res.getTimestamp(++k);
                String technologie 	= res.getString(++k);
                String tfGroup	 	= res.getString(++k);
                String werkNo     	= res.getString(++k);
                String name       	= res.getString(++k);
                String ziel       	= res.getString(++k);
                String zeitstelle 	= res.getString(++k);
                String segmentNo  	= res.getString(++k);
                Date creationTime   = res.getTimestamp(++k);

                Lokation lok = new LokationImpl();
                lok.setId(lokId);
                lok.setWerknummer(werkNo);
                lok.setName(name);
                lok.setZielArt(ziel);
                lok.setWESZeitstelle(zeitstelle);
                lok.setWESSegmentNr(segmentNo);

                Tour t = new TourImpl();
                t.setId(tourId);
                t.setBorderonummer(bordero);
                t.setLokation(lok);
                t.setETA1(eta1);
                t.setETA2(eta2);
                t.setVerzoegerung(delay);
                t.setStatus(tourStatus);
                t.setZeitfensterStart(zfStart);
                t.setZeitfensterEnde(zfEnde);
                t.setTechnologie(technologie);
                t.setTimeframeGroup(tfGroup);
                t.setCreationTime(creationTime);

                l.add(t);
            }
        }
        catch (SQLException se)
        {
            se.printStackTrace();
            throw new DAOException("Could not read Touren Result Set" + se.getMessage()
                                   + " caused by: " + ChainedSQLExceptionWalker.getAllMessages(se));
        }
        return l;
    }

    /** selects a list of Koordinaten during an existing connection
     *
     * @param dbAccess
     * @param connection
     * @param searchParams
     * @param sqlClause
     * @return
     * @throws DAOException
     * @throws SQLException
     */
    public static List selectKoordinaten(DefaultDBAccess dbAccess, Connection connection, HashMap searchParams, String sqlClause) throws DAOException, SQLException{

        List l = null;
        Map p = searchParams;

        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, loggingPrefix + "select Koordinaten: " + p);
        }
        SQLStatement stmt = dbAccess.createPreparedStatement(sqlClause,connection,p);

        ResultSet res = stmt.getPreparedStatement().executeQuery();
        l = convertKoordinatenResultSet(res);
        stmt.getPreparedStatement().close();

        return l;
    }

    /** converts the ResultSet into a list of Koordinaten
     *
     * @param res
     * @return
     * @throws DAOException
     */
    private static List convertKoordinatenResultSet(ResultSet res) throws DAOException {
        if (res == null) {
            return null;
        }
        List l = new ArrayList();
        try
        {
            while (res.next())
            {
                long id         = res.getLong(1);
                //long lokId      = res.getLong(2);
                int laengenGrad = res.getInt(3);
                int breitenGrad = res.getInt(4);
                int reihenfolge = res.getInt(5);

                Koordinaten k = new KoordinatenImpl();
                k.setId(id);
                k.setLaengengrad(laengenGrad);
                k.setBreitengrad(breitenGrad);
                k.setReihenfolge(reihenfolge);

                l.add(k);
            }
        }
        catch (SQLException se)
        {
            se.printStackTrace();
            throw new DAOException("Could not read Koordinaten Result Set" + se.getMessage()
                                   + " caused by: " + ChainedSQLExceptionWalker.getAllMessages(se));
        }
        return l;
    }

    /** creates a connection to the data base and selects a list of Lokationen
     *
     * @param searchParams
     * @param sqlClause
     * @return
     * @throws DAOException
     */
    private List retrieveLokationen(HashMap searchParams, String sqlClause) throws DAOException {
        logger.log(Level.INFO, loggingPrefix + "1329"+"|" + "starting retrieveLokationen");
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);

        dbAccess.setMaxNumberOfRows(0);
        dbAccess.addUnresolvedFilterListener(this);
        dbAccess.addUnresolvedParameterListener(this);

        Connection connection = null;
        List l = null;
        try {
            connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);

            try {
                l = selectLokationen(dbAccess, connection, searchParams, sqlClause);
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                String appender = "";
                if (e instanceof SQLException) {
                    appender =  " caused by: " + ChainedSQLExceptionWalker.getAllMessages((SQLException) e);
                }
                throw new DAOException("Error selecting Lokationen: "
                                       + e.getMessage() + appender);
            }

        } catch (SQLException e) {
            logger.log(Level.SEVERE, loggingPrefix + "1330"+"|" + "Could not retrieve Lokationen: " + ChainedSQLExceptionWalker.getAllMessages(e), e);
            throw new DAOException("Could not retrieve Lokationen:"+ e.getMessage()
                                   + " caused by: " + ChainedSQLExceptionWalker.getAllMessages(e));
        } finally {
            if (connection != null) {
                dbAccess.closeConnection(connection);
            }
        }
        return l;
    }

    /** selects a list of Lokationen during an existing connection
     *
     * @param dbAccess
     * @param connection
     * @param searchParams
     * @param sqlClause
     * @return
     * @throws DAOException
     * @throws SQLException
     */
    private List selectLokationen(DefaultDBAccess dbAccess, Connection connection, HashMap searchParams, String sqlClause) throws DAOException, SQLException{

        List l = null;
        Map p = searchParams;

        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, loggingPrefix + "select Lokationen: " + p);
        }
        SQLStatement stmt = dbAccess.createPreparedStatement(sqlClause,connection,p);

        ResultSet res = stmt.getPreparedStatement().executeQuery();
        l = convertLokationenResultSet(res);
        res.close();
        stmt.getPreparedStatement().close();

        //adding Koordinaten
        if (l != null && !l.isEmpty()) {
            HashMap params = new HashMap();
            for (int i = 0; i < l.size(); i++) {
                Lokation lok = (Lokation)l.get(i);
                params.put(PARAM_LOKATION_ID, new Long(lok.getId()));
                lok.setKoordinaten(selectKoordinaten(dbAccess, connection, params, SELECT_LOKATION_KOORDINATEN));
            }
        }

        return l;
    }

    /** converts the ResultSet into a list of Lokationen
     *
     * @param res
     * @return
     * @throws DAOException
     */
    private List convertLokationenResultSet(ResultSet res) throws DAOException {

        List l = new ArrayList();
        try
        {
            while (res.next())
            {
                long lokId        = res.getLong(1);
                String werkNo     = res.getString(2);
                String name       = res.getString(3);
                String ziel       = res.getString(4);
                String zeitstelle = res.getString(5);
                String segmentNo  = res.getString(6);

                Lokation lok = new LokationImpl();
                lok.setId(lokId);
                lok.setWerknummer(werkNo);
                lok.setName(name);
                lok.setZielArt(ziel);
                lok.setWESZeitstelle(zeitstelle);
                lok.setWESSegmentNr(segmentNo);

                l.add(lok);
            }
        }
        catch (SQLException se)
        {
            se.printStackTrace();
            throw new DAOException("Could not read Lokation Result Set" + se.getMessage()
                                   + " caused by: " + ChainedSQLExceptionWalker.getAllMessages(se));
        }
        return l;
    }

    /**
     * @see com.tsystems.dqm.tmgmt.tourenverwaltung.dao.TourDAO#getTourenByBorderovorsatz(java.lang.String, java.lang.String)
     */
    public List getTourenByBorderovorsatz(
            String borderovorsatz,
            String werknummer)
            throws DAOException {

        if (borderovorsatz == null || werknummer == null) {
            logger.log(Level.SEVERE, loggingPrefix + "1331"+"|" + "search parameter missing, could not select from db:\n\tborderovorsatz = "+borderovorsatz
                                     +"\n\twerknummer = "+werknummer);
            throw new DAOIllegalArgumentException("Could not load Touren from database: search parameter missing:\n\tborderovorsatz = "+borderovorsatz
                                                  +"\n\twerknummer = "+werknummer);
        }

        HashMap searchParams = new HashMap();
        searchParams.put(PARAM_BORDERONUMMER, borderovorsatz+"%");
        searchParams.put(PARAM_WERKNUMMER, werknummer);
        return retrieveTour(searchParams, SELECT_TOUREN);
    }

    /**
     * @see com.tsystems.dqm.tmgmt.tourenverwaltung.dao.TourDAO#getTourenByBorderovorsatzAndStatus(java.lang.String, java.lang.String, int)
     */
    public List getTourenByBorderovorsatzAndStatus(
            String borderovorsatz,
            String werknummer,
            int status)
            throws DAOException {

        if (borderovorsatz == null || werknummer == null) {
            logger.log(Level.SEVERE, loggingPrefix + "1332"+"|" + "search parameter missing, could not select from db:\n\tborderovorsatz = "+borderovorsatz
                                     +"\n\twerknummer = "+werknummer);
            throw new DAOIllegalArgumentException("Could not load Touren from database: search parameter missing:\n\tborderovorsatz = "+borderovorsatz
                                                  +"\n\twerknummer = "+werknummer);
        }

        HashMap searchParams = new HashMap();
        searchParams.put(PARAM_BORDERONUMMER, borderovorsatz+"%");
        searchParams.put(PARAM_WERKNUMMER, werknummer);
        searchParams.put(PARAM_STATUS, new Integer(status));
        return retrieveTour(searchParams, SELECT_TOUREN_BY_STATUS);
    }

    /**
     * @see com.tsystems.dqm.tmgmt.tourenverwaltung.dao.TourDAO#getBorderoNummernByBorderoVorsatz(java.lang.String, java.lang.String)
     */
    public List getBorderoNummernByBorderoVorsatz(String borderovorsatz, String werknummer) throws DAOException {

        if (borderovorsatz == null || werknummer == null) {
            logger.log(Level.SEVERE, loggingPrefix + "1333"+"|" +
                                     "search parameter missing, could not select from db:\n\tborderovorsatz = " + borderovorsatz + "\n\twerknummer = " + werknummer);
            throw new DAOIllegalArgumentException(
                    "Could not load Bordero Nummern from database: search parameter missing:\n\tborderovorsatz = "
                    + borderovorsatz
                    + "\n\twerknummer = "
                    + werknummer);
        }

        logger.log(Level.INFO, loggingPrefix + "1334"+"|" + "starting getBorderoNummernByBorderoVorsatz");
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);

        dbAccess.setMaxNumberOfRows(0);
        dbAccess.addUnresolvedFilterListener(this);
        dbAccess.addUnresolvedParameterListener(this);

        Map p = new HashMap();
        p.put(PARAM_BORDERONUMMER, borderovorsatz + "%");
        p.put(PARAM_WERKNUMMER, werknummer);

        Connection connection = null;
        List l = new ArrayList();
        try {
            connection = dbAccess.openConnection(Connection.TRANSACTION_READ_UNCOMMITTED);
            connection.setAutoCommit(false);

            try {
                SQLStatement stmt = dbAccess.createPreparedStatement(SELECT_BORDERONUMMER, connection, p);
                ResultSet res = stmt.getPreparedStatement().executeQuery();
                Date deadline = new Date();
                long now = deadline.getTime();
                deadline.setTime(now - 1000*60*60*24*DAYS_FOR_DEADLINE);

                while (res.next()) {
                    // try to create a new Object
                    String date = res.getString(1);
                    String time = res.getString(2);

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                    Date eintreffdatumsoll = sdf.parse(date + time);

                    logger.log(Level.INFO, loggingPrefix + "1335"+"|" + "checking date: " + eintreffdatumsoll);

                    if (eintreffdatumsoll.after(deadline)) {
                        String nummer = res.getString(3);
                        Object helper = res.getObject(4);
                        Long tourId = null;
                        int tourStatus;
                        if (helper == null) {
                            tourStatus = BorderoNummer.STATUS_NICHTVORHANDEN;
                        } else {
                            tourId = (Long) helper;
                            tourStatus = res.getInt(5);
                        }

                        BorderoNummer borderoNummer = new BorderoNummerImpl();
                        borderoNummer.setBorderoNummer(nummer);
                        borderoNummer.setTourId(tourId);
                        borderoNummer.setTourStatus(tourStatus);
                        borderoNummer.setTimeframeGroup(res.getString(6));
                        borderoNummer.setTourCreationTime(res.getTimestamp(7));

                        if (logger.isLoggable(Level.FINE)) {
                            logger.log(Level.FINE, loggingPrefix + "Found Borderonumber: " + borderoNummer);
                        }

                        l.add(borderoNummer);
                    }
                }
                stmt.getPreparedStatement().close();
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                String appender = "";
                if (e instanceof SQLException) {
                    appender =  " caused by: " + ChainedSQLExceptionWalker.getAllMessages((SQLException) e);
                }
                throw new DAOException("Error selecting Tour: "
                                       + e.getMessage() + appender);
            }

        } catch (SQLException e) {
            logger.log(Level.SEVERE, loggingPrefix + "1336"+"|" + "Could not retrieve Tour: " + ChainedSQLExceptionWalker.getAllMessages(e), e);
            throw new DAOException("Could not retrieve Tour:"+ e.getMessage()
                                   + " caused by: " + ChainedSQLExceptionWalker.getAllMessages(e));
        } finally {
            if (connection != null) {
                dbAccess.closeConnection(connection);
            }
        }
        return l;
    }

    /**
     * Returns a new 'Borderonummer' from database.
     *
     * @return
     * @throws DAOException
     *
     */
    public String getNewBorderoNummer(String werknummer) throws DAOException{
        String result = null;

        logger.log(Level.INFO, loggingPrefix + "1337"+"|" + "starting getNewBorderoNummer");
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);

        dbAccess.setMaxNumberOfRows(0);
        dbAccess.addUnresolvedFilterListener(this);
        dbAccess.addUnresolvedParameterListener(this);

        Connection connection = null;
        Statement st = null;
        try {

//			connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
            connection = dbAccess.openConnection(Connection.TRANSACTION_NONE);

            String sql = "SELECT NEXTVAL FOR S0XCTM.S0XBN_BORDERONR_" + werknummer + " FROM SYSIBM.SYSDUMMY1";
            st = connection.createStatement();
            ResultSet res = st.executeQuery(sql);

            if(res.next()) {
                result = res.getString(1);
                logger.log(Level.INFO, loggingPrefix + "1338"+"|" + "new borderonummer: " + result);
            }
//			connection.commit();

        } catch (Exception e) {
//			connection.rollback();
            e.printStackTrace();
            if(e instanceof SQLException){
                logger.log(Level.SEVERE, loggingPrefix + "1339"+"|" + "Could not retrieve new Borderonummer: " + ChainedSQLExceptionWalker.getAllMessages((SQLException)e), e);
            }
            throw new DAOException("Could not retrieve new Borderonummer:"+ e.getMessage());
        } finally {
            if(st != null){
                try{
                    st.close();
                }
                catch (Exception ex){
                    System.out.print("Cann not close the sql statement");
                }

            }
            if (connection != null)
                dbAccess.closeConnection(connection);
        }
        return result;
    }

    /**
     * Returns the generation interval of 'Borderonummer' from database.
     *
     * @return
     * @throws DAOException
     *
     */
    public long[] getBorderonummerGenerationInterval(String werknummer) throws DAOException{
        long[] result = new long[2];

        logger.log(Level.INFO, loggingPrefix + "1337"+"|" + "starting getNewBorderoNummer");
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);

        dbAccess.setMaxNumberOfRows(0);
        dbAccess.addUnresolvedFilterListener(this);
        dbAccess.addUnresolvedParameterListener(this);

        Connection connection = null;
        Statement st = null;
        try {
//			connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
            connection = dbAccess.openConnection(Connection.TRANSACTION_NONE);

            String sql = "SELECT MINVALUE, MAXVALUE FROM syscat.SEQUENCES " +
                         "WHERE seqname = 'S0XBN_BORDERONR_" + werknummer + "'";

            st = connection.createStatement();
            ResultSet res = st.executeQuery(sql);

            if(res.next()) {

                result[0] = res.getLong(1);
                result[1] = res.getLong(2);
                logger.log(Level.INFO, loggingPrefix + "1338"+"|" + "generation interval of borderonummer: [" + result[0] + "," + result[1] + "]");
            }
//			connection.commit();

        } catch (Exception e) {
            e.printStackTrace();
//			connection.rollback();
            if(e instanceof SQLException){
                logger.log(Level.SEVERE, loggingPrefix + "1339"+"|" + "Could not retrieve the generation interval of borderonummer: " + ChainedSQLExceptionWalker.getAllMessages((SQLException)e), e);
            }
            throw new DAOException("Could not retrieve the generation interval of borderonummer:"+ e.getMessage());
        } finally {
            if(st != null){
                try{
                    st.close();
                }
                catch (Exception ex){
                    System.out.print("Cann not close the sql statement");
                }

            }
            if (connection != null)
                dbAccess.closeConnection(connection);
        }
        return result;
    }

    /**
     * Delete all tours before a given date.
     * @return
     * the delete count or -1 if an error occurred.
     */
    public int deleteTours(Date deleteBefore) throws DAOException{
        String result = null;

        logger.log(Level.INFO, loggingPrefix + "1340"+"|" + "delete Tours before "+deleteBefore);
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null, null);

        dbAccess.setMaxNumberOfRows(0);
        dbAccess.addUnresolvedFilterListener(this);
        dbAccess.addUnresolvedParameterListener(this);

        Connection connection = null;
        PreparedStatement st = null;
        try {

//			connection = dbAccess.openConnection(Connection.TRANSACTION_READ_COMMITTED);
            connection = dbAccess.openConnection(Connection.TRANSACTION_NONE);

            String sql = "DELETE FROM S0XCTM.S0XRTOUR_TOUR WHERE CREATION_TIME < ?";
            st = connection.prepareStatement(sql);
            st.setDate(1, new java.sql.Date(deleteBefore.getTime()));
            int deleteCount = st.executeUpdate();

            logger.log(Level.INFO, loggingPrefix + "1338"+"|" + "deleted " + deleteCount + " records.");
            return deleteCount;

        } catch (Exception e) {
            e.printStackTrace();
            if(e instanceof SQLException){
                logger.log(Level.SEVERE, loggingPrefix + "1339"+"|" + "Could not delete tours " + ChainedSQLExceptionWalker.getAllMessages((SQLException)e), e);
            }
            throw new DAOException("Could not delete tours: "+ e.getMessage());
        } finally {
            if(st != null){
                try{
                    st.close();
                }
                catch (Exception ex){
                    System.out.print("Cann not close the sql statement");
                }

            }
            if (connection != null)
                dbAccess.closeConnection(connection);
        }
    }

}