package com.tsystems.dqm.tmgmt.beladeliste.dao;

import com.tsystems.dao.xml.DAOException;
import com.tsystems.dao.xml.config.ConfigLoader;
import com.tsystems.dao.xml.db.DefaultDBAccess;
import com.tsystems.dao.xml.db.SQLStatement;
import com.tsystems.dao.xml.db.UnresolvedFilterEvent;
import com.tsystems.dao.xml.db.UnresolvedFilterListener;
import com.tsystems.dao.xml.db.UnresolvedParameterEvent;
import com.tsystems.dao.xml.db.UnresolvedParameterListener;
import com.tsystems.dqm.pai.DALLoggingConstants;
import com.tsystems.dqm.security.util.CheckAuthorization;
import com.tsystems.dqm.security.util.SecurityNotPresentException;
import com.tsystems.dqm.tmgmt.beladeliste.dao.BeladelisteFingerprint.FingerprintLine;
import com.tsystems.dqm.tmgmt.beladeliste.impl.BeladelisteImpl;
import com.tsystems.dqm.tmgmt.beladeliste.impl.BeladelistePrimaryKeyImpl;
import com.tsystems.dqm.tmgmt.beladeliste.impl.LieferscheinImpl;
import com.tsystems.dqm.tmgmt.beladeliste.impl.LieferscheinPositionImpl;
import com.tsystems.dqm.tmgmt.beladeliste.impl.PackstueckPositionImpl;
import com.tsystems.dqm.tmgmt.beladeliste.impl.SendungImpl;
import com.tsystems.dqm.tmgmt.beladeliste.interfaces.Beladeliste;
import com.tsystems.dqm.tmgmt.beladeliste.interfaces.BeladelistePrimaryKey;
import com.tsystems.dqm.tmgmt.beladeliste.interfaces.Lieferschein;
import com.tsystems.dqm.tmgmt.beladeliste.interfaces.LieferscheinPosition;
import com.tsystems.dqm.tmgmt.beladeliste.interfaces.PackstueckPosition;
import com.tsystems.dqm.tmgmt.beladeliste.interfaces.Sendung;
import com.tsystems.util.expr.ConstantList;
import com.tsystems.utils.exceptions.ChainedSQLExceptionWalker;
import org.dom4j.DocumentException;
import org.xml.sax.SAXException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Class BeladelisteDB.
 *
 * @author bmessers
 */
public class BeladelisteDB implements UnresolvedFilterListener,
        UnresolvedParameterListener, BeladelisteDAO,
        BeladelistePrimaryKeyBuilder {

    /** The Constant logger. */
    private static final Logger logger = Logger.getLogger(BeladelisteDB.class
            .getName());

    /** The logging prefix. */
    private static String loggingPrefix = DALLoggingConstants.PAI_PROJ_DQMDATAACCESSEJB_ID;

    /** The config. */
    private transient BeladelisteConfigLoader config = null;

    /** The Constant DB_CONFIG_PATH. */
    private static final String DB_CONFIG_PATH = "com/tsystems/dqm/tmgmt/beladeliste";

    /** The Constant DB_CONFIG_FILE. */
    private static final String DB_CONFIG_FILE = "Beladeliste.xml";

    /** The Constant CONTEXT. */
    private static final String CONTEXT = "Beladeliste";

    /** The Constant INSERT_BELADELISTE. */
    private static final String INSERT_BELADELISTE = "INSERT.BELADELISTE";

    /** The Constant UPDATE_BELADELISTE. */
    private static final String UPDATE_BELADELISTE = "UPDATE.BELADELISTE";

    /** The Constant SELECT_BELADELISTE. */
    private static final String SELECT_BELADELISTE = "SELECT.BELADELISTE";

    /** The Constant SELECT_PACKSTUECK_AVIS. */
    private static final String SELECT_PACKSTUECK_AVIS = "SELECT.PACKSTUECK_AVIS";

    /** The Constant SELECT_LIEFERSCHEIN_AVIS. */
    private static final String SELECT_LIEFERSCHEIN_AVIS = "SELECT.LIEFERSCHEIN_AVIS";

    /** The Constant SELECT_PACKSTUECK_AVIS_WITH_JIS. */
    private static final String SELECT_PACKSTUECK_AVIS_WITH_JIS = "SELECT.PACKSTUECK_AVIS_WITH_JIS";

    /** The Constant SELECT_SCANNED. */
    private static final String SELECT_SCANNED = "SELECT.SCANNED";

    /** The Constant INSERT_SCANNED. */
    private static final String INSERT_SCANNED = "INSERT.SCANNED";

    /** The Constant DELETE_SCANNED. */
    private static final String DELETE_SCANNED = "DELETE.SCANNED";

    /** The Constant INSERT_FINGERPRINT. */
    private static final String INSERT_FINGERPRINT = "INSERT.FINGERPRINT";

    /** The Constant SELECT_FINGERPRINT. */
    private static final String SELECT_FINGERPRINT = "SELECT.FINGERPRINT";

    /** The Constant SELECT_ZOLLGUT_STATUS. */
    private static final String SELECT_ZOLLGUT_STATUS = "SELECT.ZOLLGUT_STATUS";

    /** The Constant SELECT_SACHNUMMERN. */
    private static final String SELECT_SACHNUMMERN = "SELECT.SACHNUMMERN";

    /** The Constant BATCH_COMMIT_COUNT. */
    private static final int BATCH_COMMIT_COUNT = 100;

    /** The Constant PARAM_BORDERONUMMER. */
    private static final String PARAM_BORDERONUMMER = "BORDERONUMMER";

    /** The Constant PARAM_BORDERONUMMER_9. */
    private static final String PARAM_BORDERONUMMER_9 = "BORDERONUMMER_9";

    /** The Constant PARAM_WERKNUMMER. */
    private static final String PARAM_WERKNUMMER = "WERKNUMMER";

    /** The Constant PARAM_VERSION. */
    private static final String PARAM_VERSION = "VERSION";

    /** The Constant PARAM_TRANSPORT_ID. */
    private static final String PARAM_TRANSPORT_ID = "TRANSPORT_ID";

    /** The Constant PARAM_SENDUNG_ID. */
    private static final String PARAM_SENDUNG_ID = "SENDUNG_ID";

    /** The Constant PARAM_SCANZEITPUNKT. */
    private static final String PARAM_SCANZEITPUNKT = "SCANZEITPUNKT";

    /** The Constant PARAM_LIEFERANTENNUMMER. */
    private static final String PARAM_LIEFERANTENNUMMER = "LIEFERANTENNUMMER";

    /** The Constant PARAM_PACKSTUECKNUMMER. */
    private static final String PARAM_PACKSTUECKNUMMER = "PACKSTUECKNUMMER";

    /** The Constant FILTER_IDS. */
    private static final String FILTER_IDS = "IDS";

    /** The counter1. */
    private static int counter1 = 1;
    // use the EXACT order as is in XML !!!!
    /** The Constant COL_ID. */
    private static final int COL_ID = counter1++;

    /** The Constant COL_SNR. */
    private static final int COL_SNR = counter1++;

    /** The Constant COL_UNIT. */
    private static final int COL_UNIT = counter1++;

    /** The Constant COL_PID. */
    private static final int COL_PID = counter1++;

    /** The counter2. */
    private static int counter2 = 1;
    // use the EXACT order as is in XML !!!!
    /** The Constant COL_TID. */
    private static final int COL_TID = counter2++;

    /** The Constant COL_SENDUNGSLADUNGSBEZUGSNUMMER. */
    private static final int COL_SENDUNGSLADUNGSBEZUGSNUMMER = counter2++;

    /** The Constant COL_DATENERSTELLERNUMMER. */
    private static final int COL_DATENERSTELLERNUMMER = counter2++;

    /** The Constant COL_S_ID. */
    private static final int COL_S_ID = counter2++;

    /** The Constant COL_LIEFERSCHEINNUMMER. */
    private static final int COL_LIEFERSCHEINNUMMER = counter2++;

    /** The Constant COL_HERSTELLERNUMMER. */
    private static final int COL_HERSTELLERNUMMER = counter2++;

    /** The Constant COL_LS_ID. */
    private static final int COL_LS_ID = counter2++;

    /** The Constant COL_LIEFERSCHEINPOSITIONSNUMMER. */
    private static final int COL_LIEFERSCHEINPOSITIONSNUMMER = counter2++;

    /** The Constant COL_LP_ID. */
    private static final int COL_LP_ID = counter2++;

    /** The Constant COL_PACKSTUECKNUMMER. */
    private static final int COL_PACKSTUECKNUMMER = counter2++;

    /** The Constant COL_PP_ID. */
    private static final int COL_PP_ID = counter2++;

    /** The Constant COL_PACKMITTELNUMMER. */
    private static final int COL_PACKMITTELNUMMER = counter2++;

    /** The Constant COL_ANZAHLPACKMITTEL. */
    private static final int COL_ANZAHLPACKMITTEL = counter2++;

    /** The Constant COL_PACKSTUECKNUMMERVON. */
    private static final int COL_PACKSTUECKNUMMERVON = counter2++;

    /** The Constant COL_PACKSTUECKNUMMERBIS. */
    private static final int COL_PACKSTUECKNUMMERBIS = counter2++;

    /** The Constant COL_LABELKENNUNG. */
    private static final int COL_LABELKENNUNG = counter2++;

    /** The Constant COL_MASTERPACKSTUECKNUMMER. */
    private static final int COL_MASTERPACKSTUECKNUMMER = counter2++;

    /** The Constant COL_DUBLETTE. */
    private static final int COL_DUBLETTE = counter2++;

    /** The Constant COL_LADUNGSTRAEGERPOSITIONSNR. */
    private static final int COL_LADUNGSTRAEGERPOSITIONSNR = counter2++;

    /** The Constant COL_ABLADESTELLE. */
    private static final int COL_ABLADESTELLE = counter2++;

    /** The Constant COL_PACKMITTELNUMMERLIEFERANT. */
    private static final int COL_PACKMITTELNUMMERLIEFERANT = counter2++;

    /** The Constant COL_LAGERABRUFNUMMER. */
    private static final int COL_LAGERABRUFNUMMER = counter2++;

    /** The Constant COL_VERPACKUNGSKENNUNG. */
    private static final int COL_VERPACKUNGSKENNUNG = counter2++;

    /** The Constant COL_EIGENTUMSKENNUNG. */
    private static final int COL_EIGENTUMSKENNUNG = counter2++;

    /** The Constant COL_POSITIONSNUMMERLIEFERSCHEIN. */
    private static final int COL_POSITIONSNUMMERLIEFERSCHEIN = counter2++;

    /** The Constant COL_STATUS. */
    private static final int COL_STATUS = counter2++;

    /** The Constant COL_KENNZEICHENULUEL. */
    private static final int COL_KENNZEICHENULUEL = counter2++;

    /** The Constant COL_CHARGENNUMMER. */
    private static final int COL_CHARGENNUMMER = counter2++;

    /** The Constant COL_GEOMETRIELAENGE. */
    private static final int COL_GEOMETRIELAENGE = counter2++;

    /** The Constant COL_GEOMETRIEBREITE. */
    private static final int COL_GEOMETRIEBREITE = counter2++;

    /** The Constant COL_GEOMETRIEHOEHE. */
    private static final int COL_GEOMETRIEHOEHE = counter2++;

    /** The Constant COL_GEOMETRIEGEWICHT. */
    private static final int COL_GEOMETRIEGEWICHT = counter2++;

    /** The Constant COL_GEOMETRIEFUELLMENGE. */
    private static final int COL_GEOMETRIEFUELLMENGE = counter2++;

    /** The Constant COL_STAPELFAKTOR. */
    private static final int COL_STAPELFAKTOR = counter2++;

    /** The Constant COL_LIEFERSCHEINPACKSTUECKREF. */
    private static final int COL_LIEFERSCHEINPACKSTUECKREF = counter2++;

    /** The Constant COL_TRANSPORTPARTNERNUMMER. */
    private static final int COL_TRANSPORTPARTNERNUMMER = counter2++;

    /** The Constant COL_TRANSPORTMITTELNUMMER. */
    private static final int COL_TRANSPORTMITTELNUMMER = counter2++;

    /** The Constant COL_KFZKENNZEICHEN. */
    private static final int COL_KFZKENNZEICHEN = counter2++;

    /** The Constant DEFAULT_MAX_UNIT_COMPRESSION_COUNT. */
    public final static int DEFAULT_MAX_UNIT_COMPRESSION_COUNT = 10000;

    /**
     * The Class BeladelisteConfigLoader.
     */
    class BeladelisteConfigLoader extends ConfigLoader {

        /**
         * Instantiates a new beladeliste config loader.
         */
        public BeladelisteConfigLoader() {
            super(BeladelisteConfigLoader.class.getClassLoader());
        }

        /**
         * Load default.
         *
         * @throws SAXException the SAX exception
         * @throws DocumentException the document exception
         */
        public void loadDefault() throws SAXException, DocumentException {
            logger.log(Level.INFO, loggingPrefix + "1785" + "|" + "Path = "
                                   + DB_CONFIG_PATH);
            load(DB_CONFIG_PATH, DB_CONFIG_FILE);
        }
    }

    /**
     * Initializes the configuration.
     */
    private void initConfigLoader() {
        if (config == null) {
            logger.log(Level.INFO, loggingPrefix + "1786" + "|"
                                   + "initialize ConfigLoader");
            config = new BeladelisteConfigLoader();
            try {
                config.loadDefault();
            } catch (Exception ex) {
                logger.log(Level.SEVERE, loggingPrefix + "1014" + "|"
                                         + "Could not load configuration: " + ex.getMessage(),
                        ex);
            }
        }
    }

    /*
     * (Kein Javadoc)
     *
     * @see
     * com.tsystems.dao.xml.db.UnresolvedFilterListener#unresolvedFilter(com
     * .tsystems.dao.xml.db.UnresolvedFilterEvent)
     */
    /* (non-Javadoc)
     * @see com.tsystems.dao.xml.db.UnresolvedFilterListener#unresolvedFilter(com.tsystems.dao.xml.db.UnresolvedFilterEvent)
     */
    public void unresolvedFilter(UnresolvedFilterEvent e) {
        UnresolvedFilterEvent.unresolvedFilter(e);
    }

    /*
     * (Kein Javadoc)
     *
     * @see
     * com.tsystems.dao.xml.db.UnresolvedParameterListener#unresolvedParameter
     * (com.tsystems.dao.xml.db.UnresolvedParameterEvent)
     */
    /* (non-Javadoc)
     * @see com.tsystems.dao.xml.db.UnresolvedParameterListener#unresolvedParameter(com.tsystems.dao.xml.db.UnresolvedParameterEvent)
     */
    public void unresolvedParameter(UnresolvedParameterEvent e) {
    }

    /*
     * (Kein Javadoc)
     *
     * @see com.tsystems.dqm.tmgmt.beladeliste.dao.BeladelisteDAO#
     * getLoadingListByDeliveryNotesOnly(java.lang.String, java.lang.String)
     */
    /* (non-Javadoc)
     * @see com.tsystems.dqm.tmgmt.beladeliste.dao.BeladelisteDAO#getLoadingListByDeliveryNotesOnly(java.lang.String, java.lang.String)
     */
    public Beladeliste getLoadingListByDeliveryNotesOnly(String borderonummer,
                                                         String werknummer) throws DAOException {
        if (logger.isLoggable(Level.INFO)) {
            logger.log(
                    Level.INFO,
                    loggingPrefix
                    + "1015"
                    + "|"
                    + "Starting getLoadingListByDeliveryNotesOnly with bordero "
                    + borderonummer + " and plant " + werknummer);
        }

        if (borderonummer == null || werknummer == null
            || borderonummer.length() < 8) {
            logger.log(Level.SEVERE, loggingPrefix + "1016" + "|"
                                     + "Some Parameters do not match the requirements!");
            throw new DAOException(
                    "Some Parameters do not match the requirements! borderonummer="
                    + borderonummer + ", werknummer=" + werknummer);
        }

        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null,
                null);
        dbAccess.setMaxNumberOfRows(0);
        dbAccess.addUnresolvedFilterListener(this);
        dbAccess.addUnresolvedParameterListener(this);

        Connection connection = null;
        try {
            connection = dbAccess
                    .openConnection(Connection.TRANSACTION_READ_COMMITTED);

            Map p = new HashMap(5);
            p.put(PARAM_WERKNUMMER, werknummer);
            p.put(PARAM_BORDERONUMMER, borderonummer);
            p.put(PARAM_BORDERONUMMER_9, borderonummer.substring(0, 8) + "_");

            try {
                CheckAuthorization checkAuthorization = new CheckAuthorization();
                Map grants = checkAuthorization.getWriteGrants();
                dbAccess.addFilters(grants, true, p);
            } catch (SecurityNotPresentException e) {
                logger.log(
                        Level.WARNING,
                        loggingPrefix
                        + "1017"
                        + "|"
                        + "No subject present ... performing an insecure request.");
            }

            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, loggingPrefix
                                       + "Getting loading list with: " + p);
            }

            SQLStatement stmt = null;
            int version = 0;
            try {
                stmt = dbAccess.createPreparedStatement(SELECT_BELADELISTE,
                        connection, p);
                ResultSet versionResult = dbAccess
                        .executePreparedStatement(stmt.getPreparedStatement());
                if (versionResult.next()) {
                    version = versionResult.getInt(1);
                } else {
                    version = 1;
                }
            } catch (DAOException sqlex) {
                logger.log(Level.SEVERE,
                        loggingPrefix + "1020" + "|"
                        + "Failed to read unloading list version: "
                        + sqlex.getMessage(), sqlex);
            } finally {
                if ((stmt != null) && (stmt.getPreparedStatement() != null)) {
                    stmt.getPreparedStatement().close();
                }
            }

            stmt = dbAccess.createPreparedStatement(SELECT_LIEFERSCHEIN_AVIS,
                    connection, p);
            ResultSet avisResult = dbAccess.executePreparedStatement(stmt
                    .getPreparedStatement());

            BeladelisteFingerprint fp = new BeladelisteFingerprint();

            Beladeliste beladeliste = createAvisBeladeliste(avisResult, fp,
                    false);
            stmt.getPreparedStatement().close();

            beladeliste.setWerknummer(werknummer);
            beladeliste.setBorderoNummer(borderonummer);

            beladeliste.setVersion(version);

            return beladeliste;

        } catch (SQLException exc) {
            String txt = "Could not load new Beladeliste from database: "
                         + exc.getMessage() + " caused by: "
                         + ChainedSQLExceptionWalker.getAllMessages(exc);
            logger.log(Level.SEVERE, loggingPrefix + "1022" + "|" + txt, exc);
            throw new DAOException(txt);
        } finally {
            if (connection != null) {
                dbAccess.closeConnection(connection);
            }
        }
    }

    /* (non-Javadoc)
     * @see com.tsystems.dqm.tmgmt.beladeliste.dao.BeladelisteDAO#isAnyDutiableGoodsFlagSet(java.lang.String, java.lang.String)
     */
    public boolean isAnyDutiableGoodsFlagSet(String bordero, String plant)
            throws DAOException {
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, loggingPrefix + "1026" + "|"
                                   + "Checking if there is any custom flag set with bordero "
                                   + bordero + " and plant " + plant);
        }
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null,
                null);
        dbAccess.setMaxNumberOfRows(0);
        dbAccess.addUnresolvedFilterListener(this);
        dbAccess.addUnresolvedParameterListener(this);

        Connection connection = null;
        try {
            connection = dbAccess
                    .openConnection(Connection.TRANSACTION_READ_COMMITTED);

            Map p = new HashMap(5);
            p.put(PARAM_WERKNUMMER, plant);
            p.put(PARAM_BORDERONUMMER, bordero);
            try {
                CheckAuthorization checkAuthorization = new CheckAuthorization();
                Map grants = checkAuthorization.getWriteGrants();
                dbAccess.addFilters(grants, true, p);
            } catch (SecurityNotPresentException e) {
                logger.log(
                        Level.WARNING,
                        loggingPrefix
                        + "1028"
                        + "|"
                        + "No subject present ... performing an insecure request.");
            }
            SQLStatement stmt = dbAccess.createPreparedStatement(
                    SELECT_ZOLLGUT_STATUS, connection, p);
            ResultSet result = dbAccess.executePreparedStatement(stmt
                    .getPreparedStatement());
            if (result.next()) {
                if (result.getString(1).trim().length() > 0) {
                    return true;
                }
            }
        } catch (SQLException exc) {
            String txt = "Could not load new Beladeliste from database: "
                         + exc.getMessage() + " caused by: "
                         + ChainedSQLExceptionWalker.getAllMessages(exc);
            logger.log(Level.SEVERE, loggingPrefix + "1029" + "|" + txt, exc);
            throw new DAOException(txt);
        } finally {
            if (connection != null) {
                dbAccess.closeConnection(connection);
            }
        }
        return false;
    }

    /*
     * (Kein Javadoc)
     *
     * @see
     * com.tsystems.dqm.tmgmt.beladeliste.dao.BeladelisteDAO#loadBeladeliste
     * (java.lang.String, java.lang.String)
     */
    /* (non-Javadoc)
     * @see com.tsystems.dqm.tmgmt.beladeliste.dao.BeladelisteDAO#loadBeladeliste(java.lang.String, java.lang.String, boolean, boolean)
     */
    public Beladeliste loadBeladeliste(String borderonummer, String werknummer,
                                       boolean increaseVersion, boolean includeJIS) throws DAOException {
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, loggingPrefix + "1015" + "|"
                                   + "Starting loadBeladeliste with bordero " + borderonummer
                                   + " and plant " + werknummer);
        }

        if (borderonummer == null || werknummer == null
            || borderonummer.length() < 8) {
            logger.log(Level.SEVERE, loggingPrefix + "1016" + "|"
                                     + "Some Parameters do not match the requirements!");
            throw new DAOException(
                    "Some Parameters do not match the requirements! borderonummer="
                    + borderonummer + ", werknummer=" + werknummer);
        }

        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null,
                null);
        dbAccess.setMaxNumberOfRows(0);
        dbAccess.addUnresolvedFilterListener(this);
        dbAccess.addUnresolvedParameterListener(this);

        Connection connection = null;
        try {
            connection = dbAccess
                    .openConnection(Connection.TRANSACTION_READ_COMMITTED);

            Map p = new HashMap(5);
            p.put(PARAM_WERKNUMMER, werknummer);
            p.put(PARAM_BORDERONUMMER, borderonummer);
            p.put(PARAM_BORDERONUMMER_9, borderonummer.substring(0, 8) + "_");

            try {
                CheckAuthorization checkAuthorization = new CheckAuthorization();
                Map grants = checkAuthorization.getWriteGrants();
                dbAccess.addFilters(grants, true, p);
            } catch (SecurityNotPresentException e) {
                logger.log(
                        Level.WARNING,
                        loggingPrefix
                        + "1017"
                        + "|"
                        + "No subject present ... performing an insecure request.");
            }

            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, loggingPrefix
                                       + "Getting loading list with: " + p);
            }

            SQLStatement stmt = null;
            int version = 0;
            if (increaseVersion) {
                try {
                    // first check if list exists
                    stmt = dbAccess.createPreparedStatement(SELECT_BELADELISTE,
                            connection, p);
                    ResultSet versionResult = dbAccess
                            .executePreparedStatement(stmt
                                    .getPreparedStatement());
                    if (versionResult.next()) {
                        version = versionResult.getInt(1) + 1;
                    } else {
                        stmt.getPreparedStatement().close();
                        // create Beladeliste body
                        p.put(PARAM_VERSION, Integer.valueOf(version));
                        stmt = dbAccess.createPreparedStatement(
                                INSERT_BELADELISTE, connection, p);
                        dbAccess.executePreparedStatement(stmt
                                .getPreparedStatement());
                    }
                } catch (DAOException sqlex) {
                    if (stmt != null && stmt.getPreparedStatement() != null) {
                        stmt.getPreparedStatement().close();
                    }
                    if (sqlex.getMessage().indexOf("23505") >= 0) {
                        // duplicate key
                        stmt = dbAccess.createPreparedStatement(
                                SELECT_BELADELISTE, connection, p);
                        ResultSet versionResult = dbAccess
                                .executePreparedStatement(stmt
                                        .getPreparedStatement());
                        if (versionResult.next()) {
                            version = versionResult.getInt(1) + 1;
                        } else {
                            logger.log(
                                    Level.SEVERE,
                                    loggingPrefix
                                    + "1018"
                                    + "|"
                                    + "Inconsistent database state detected for loading list version: "
                                    + p);
                        }
                    } else {
                        logger.log(Level.SEVERE, loggingPrefix + "1019" + "|"
                                                 + "Failed to create unloading list version: "
                                                 + sqlex.getMessage(), sqlex);
                    }
                } finally {
                    if ((stmt != null) && (stmt.getPreparedStatement() != null)) {
                        stmt.getPreparedStatement().close();
                    }
                }
            } else {
                try {
                    stmt = dbAccess.createPreparedStatement(SELECT_BELADELISTE,
                            connection, p);
                    ResultSet versionResult = dbAccess
                            .executePreparedStatement(stmt
                                    .getPreparedStatement());
                    if (versionResult.next()) {
                        version = versionResult.getInt(1);
                    } else {
                        version = 1;
                    }
                } catch (DAOException sqlex) {
                    logger.log(Level.SEVERE,
                            loggingPrefix + "1020" + "|"
                            + "Failed to read unloading list version: "
                            + sqlex.getMessage(), sqlex);
                } finally {
                    if ((stmt != null) && (stmt.getPreparedStatement() != null)) {
                        stmt.getPreparedStatement().close();
                    }
                }
            }

            stmt = dbAccess.createPreparedStatement(
                    (includeJIS) ? SELECT_PACKSTUECK_AVIS_WITH_JIS
                            : SELECT_PACKSTUECK_AVIS, connection, p);
            ResultSet avisResult = dbAccess.executePreparedStatement(stmt
                    .getPreparedStatement());

            BeladelisteFingerprint fp = new BeladelisteFingerprint();

            Beladeliste beladeliste = createAvisBeladeliste(avisResult, fp,
                    includeJIS);
            stmt.getPreparedStatement().close();

            beladeliste.setWerknummer(werknummer);
            beladeliste.setBorderoNummer(borderonummer);

            beladeliste.setVersion(version);

            if (version > 0) {
                stmt = dbAccess.createPreparedStatement(SELECT_SCANNED,
                        connection, p);
                ResultSet scanResult = dbAccess.executePreparedStatement(stmt
                        .getPreparedStatement());
                mergeScannedLoadingUnits(beladeliste, scanResult);
                stmt.getPreparedStatement().close();
            }

            if (increaseVersion) {
                stmt = dbAccess.createPreparedStatement(UPDATE_BELADELISTE,
                        connection, p);
                dbAccess.executePreparedStatement(stmt.getPreparedStatement());
                stmt.getPreparedStatement().close();

                stmt = null;
                int batchCount = 1;
                p.put(PARAM_VERSION, Integer.valueOf(beladeliste.getVersion()));
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, loggingPrefix
                                           + "Using FINGERPRINT '" + fp
                                           + "' for new loading list of " + borderonummer
                                           + "/" + werknummer);
                }
                for (Iterator it = fp.getLines(); it.hasNext(); batchCount++) {
                    if ((stmt != null)
                        && (batchCount % BATCH_COMMIT_COUNT == 0)) {
                        stmt.getPreparedStatement().executeBatch();
                    }
                    FingerprintLine line = (FingerprintLine) it
                            .next();
                    p.put(PARAM_TRANSPORT_ID,
                            Long.valueOf(line.getTransportID()));
                    p.put(PARAM_SENDUNG_ID, Long.valueOf(line.getSendingID()));
                    p.put(PARAM_VERSION, Integer.valueOf(version));
                    if (stmt == null) {
                        stmt = dbAccess.createPreparedStatement(
                                INSERT_FINGERPRINT, connection, p);
                    } else {
                        dbAccess.fillPreparedStatement(stmt, p);
                    }
                    stmt.getPreparedStatement().addBatch();
                }
                if (stmt != null) {
                    stmt.getPreparedStatement().executeBatch();
                    stmt.getPreparedStatement().close();
                }
            }
            return beladeliste;

        } catch (SQLException exc) {
            String txt = "Could not load new Beladeliste from database: "
                         + exc.getMessage() + " caused by: "
                         + ChainedSQLExceptionWalker.getAllMessages(exc);
            logger.log(Level.SEVERE, loggingPrefix + "1022" + "|" + txt, exc);
            throw new DAOException(txt);
        } finally {
            if (connection != null) {
                dbAccess.closeConnection(connection);
            }
        }
    }

    /**
     * Merges the scan information into the Beladeliste by setting the state of
     * loading units found in the list with status red to status green. Loading
     * units not found will be added under the Beladeliste object with status
     * yellow.
     *
     * @param beladeliste            the list of announced loading units (status red).
     * @param scanResult            the DB request result with the information about scanned
     * loading units.
     * @throws SQLException the SQL exception
     */
    private void mergeScannedLoadingUnits(Beladeliste beladeliste,
                                          ResultSet scanResult) throws SQLException {
        List supplementalUnits = new LinkedList();
        beladeliste.setPackstueckeOhneAvis(supplementalUnits);
        Map announcedLoadingUnits = new HashMap();
        for (Iterator sit = beladeliste.getBeladung().iterator(); sit.hasNext();) {
            Sendung s = (Sendung) sit.next();
            for (Iterator lit = s.getLieferscheine().iterator(); lit.hasNext();) {
                Lieferschein l = (Lieferschein) lit.next();
                Map m = (Map) announcedLoadingUnits
                        .get(l.getHerstellernummer());
                if (m == null) {
                    m = new HashMap();
                    announcedLoadingUnits.put(l.getHerstellernummer(), m);
                }
                for (Iterator pit = l.getPackstuecke().iterator(); pit
                        .hasNext();) {
                    PackstueckPosition pp = (PackstueckPosition) pit.next();
                    m.put(pp.getPackstuecknummer(), pp);
                    if (pp.getPackstuecke() != null) {
                        for (Iterator spit = pp.getPackstuecke().iterator(); spit
                                .hasNext();) {
                            PackstueckPosition spp = (PackstueckPosition) spit
                                    .next();
                            m.put(spp.getPackstuecknummer(), spp);
                        }
                    }
                }
            }
        }

        while (scanResult.next()) {

            String supplier = scanResult.getString(1);
            String loadingUnit = scanResult.getString(2);
            Timestamp scanned = scanResult.getTimestamp(3);

            Map pp = (Map) announcedLoadingUnits.get(supplier);
            if (pp == null) {
                buildSupplementalLoadingUnit(supplementalUnits, supplier,
                        loadingUnit, scanned);
            } else {
                PackstueckPosition ap = (PackstueckPosition) pp
                        .get(loadingUnit);
                if (ap == null) {
                    buildSupplementalLoadingUnit(supplementalUnits, supplier,
                            loadingUnit, scanned);
                } else {
                    ap.setStatusInBeladeliste(PackstueckPosition.STATUS_GRUEN);
                    ap.setScanzeitpunkt(scanned);
                    ap.setLieferantennummer(supplier);
                }
            }
        }

        checkMasterSingleScanningDependency(beladeliste);
    }

    /**
     * When a master was scanned and is of state 'green', all of it´s
     * successors must be green as well.
     *
     * When a single within a single-master structure is green, the related
     * master and of it´s children must be set to status 'green'.
     *
     * @param beladeliste
     * the <code>Beladeliste</code> to be processed
     */
    private void checkMasterSingleScanningDependency(Beladeliste beladeliste) {

        boolean isDebugEnabled = logger.isLoggable(Level.FINEST);

        if (isDebugEnabled) {
            logger.log(Level.FINEST, loggingPrefix + "@replace@|"
                                     + "Performing check of master/single scanning dependency");
        }

        for (Iterator sit = beladeliste.getBeladung().iterator(); sit.hasNext();) {
            Sendung s = (Sendung) sit.next();
            for (Iterator lit = s.getLieferscheine().iterator(); lit.hasNext();) {
                Lieferschein l = (Lieferschein) lit.next();
                for (Iterator pit = l.getPackstuecke().iterator(); pit
                        .hasNext();) {
                    PackstueckPosition p = (PackstueckPosition) pit.next();
                    boolean isMaster = p.isMaster();
                    List singles = p.getPackstuecke();

                    // logic only relevant for master-single combinations
                    if (isMaster && (singles != null && !singles.isEmpty())) {
                        boolean isMasterGreen = p.getStatusInBeladeliste() == PackstueckPosition.STATUS_GRUEN;

                        // when master has been scanned and is 'green'
                        // all singles not 'green' automatically turn to 'green'
                        // as well
                        if (isMasterGreen) {
                            for (Iterator i = singles.iterator(); i.hasNext();) {
                                PackstueckPosition single = (PackstueckPosition) i
                                        .next();

                                // turn all yellow to green
                                if (single.getStatusInBeladeliste() != PackstueckPosition.STATUS_GRUEN) {

                                    single.setStatusInBeladeliste(PackstueckPosition.STATUS_GRUEN);
                                    single.setScanzeitpunkt(p
                                            .getScanzeitpunkt());
                                    removeFromPackstueckeOhneAvis(beladeliste,
                                            single);
                                }
                            }// end for

                            // check if at least one child/single is 'green' and
                            // if so, set master and children to 'green'
                        } else {

                            boolean containsGreenChild = false;
                            Date greenChildScanTime = null;

                            // first check if at least one child is 'green'
                            for (Iterator i = singles.iterator(); i.hasNext();) {
                                PackstueckPosition single = (PackstueckPosition) i
                                        .next();

                                if (single.getStatusInBeladeliste() == PackstueckPosition.STATUS_GRUEN) {
                                    containsGreenChild = true;
                                    greenChildScanTime = single
                                            .getScanzeitpunkt();
                                    break;
                                }
                            }// end for

                            if (containsGreenChild) {

                                // first set master to 'green'
                                if (p.getStatusInBeladeliste() != PackstueckPosition.STATUS_GRUEN) {
                                    p.setStatusInBeladeliste(PackstueckPosition.STATUS_GRUEN);
                                    p.setScanzeitpunkt(greenChildScanTime);
                                    removeFromPackstueckeOhneAvis(beladeliste,
                                            p);
                                }

                                // then, set children to 'green'
                                for (Iterator i = singles.iterator(); i
                                        .hasNext();) {
                                    PackstueckPosition single = (PackstueckPosition) i
                                            .next();

                                    // turn all yellow to green
                                    if (single.getStatusInBeladeliste() != PackstueckPosition.STATUS_GRUEN) {

                                        single.setStatusInBeladeliste(PackstueckPosition.STATUS_GRUEN);
                                        single.setScanzeitpunkt(greenChildScanTime);
                                        removeFromPackstueckeOhneAvis(
                                                beladeliste, single);
                                    }
                                }// end for
                            }// end if
                        }// end if else
                    }// end if
                }// end for
            }// end for
        }// end for
    }

    /**
     * Removes the from packstuecke ohne avis.
     *
     * @param beladeliste the beladeliste
     * @param pp the pp
     */
    private void removeFromPackstueckeOhneAvis(Beladeliste beladeliste,
                                               PackstueckPosition pp) {
        List packstueckeOhneAvis = beladeliste.getPackstueckeOhneAvis();
        if (packstueckeOhneAvis != null) {
            for (Iterator i = packstueckeOhneAvis.iterator(); i.hasNext();) {
                PackstueckPosition pl = (PackstueckPosition) i.next();
                if (pp.getPackstuecknummer().equals(pl.getPackstuecknummer())) {
                    i.remove();
                    break;
                }
            }
        }
    }

    /**
     * Builds the supplemental loading unit.
     *
     * @param supplementalUnits the supplemental units
     * @param supplier the supplier
     * @param loadingUnit the loading unit
     * @param scanned the scanned
     */
    private void buildSupplementalLoadingUnit(List supplementalUnits,
                                              String supplier, String loadingUnit, Timestamp scanned) {
        PackstueckPosition p = new PackstueckPositionImpl();
        p.setLieferantennummer(supplier);
        p.setPackstuecknummer(loadingUnit);
        p.setScanzeitpunkt(scanned);
        p.setStatusInBeladeliste(PackstueckPosition.STATUS_GELB);
        supplementalUnits.add(p);
    }

    /* (non-Javadoc)
     * @see com.tsystems.dqm.tmgmt.beladeliste.dao.BeladelisteDAO#getAvisPackstuecke(java.lang.String, java.lang.String)
     */
    public List<PackstueckPosition> getAvisPackstuecke(String bordero,
                                                       String plant) throws DAOException {
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, loggingPrefix + "1015" + "|"
                                   + "Starting getAvisPackstuecke with bordero " + bordero
                                   + " and plant " + plant);
        }

        if (bordero == null || plant == null || bordero.length() < 8) {
            logger.log(Level.SEVERE, loggingPrefix + "1016" + "|"
                                     + "Some Parameters do not match the requirements!");
            throw new DAOException(
                    "Some Parameters do not match the requirements! borderonummer="
                    + bordero + ", werknummer=" + plant);
        }

        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null,
                null);
        dbAccess.setMaxNumberOfRows(0);
        dbAccess.addUnresolvedFilterListener(this);
        dbAccess.addUnresolvedParameterListener(this);

        Connection connection = null;
        try {
            connection = dbAccess
                    .openConnection(Connection.TRANSACTION_READ_COMMITTED);

            Map p = new HashMap(5);
            p.put(PARAM_WERKNUMMER, plant);
            p.put(PARAM_BORDERONUMMER, bordero);
            p.put(PARAM_BORDERONUMMER_9, bordero.substring(0, 8) + "_");

            try {
                CheckAuthorization checkAuthorization = new CheckAuthorization();
                Map grants = checkAuthorization.getWriteGrants();
                dbAccess.addFilters(grants, true, p);
            } catch (SecurityNotPresentException e) {
                logger.log(
                        Level.WARNING,
                        loggingPrefix
                        + "1017"
                        + "|"
                        + "No subject present ... performing an insecure request.");
            }

            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, loggingPrefix
                                       + "Getting loading list with: " + p);
            }

            SQLStatement stmt = dbAccess.createPreparedStatement(
                    SELECT_PACKSTUECK_AVIS_WITH_JIS, connection, p);
            ResultSet avisResult = dbAccess.executePreparedStatement(stmt
                    .getPreparedStatement());
            List<PackstueckPosition> loadingList = createAvisLoadingList(avisResult);
            stmt.getPreparedStatement().close();

            return loadingList;

        } catch (SQLException exc) {
            String txt = "Could not load new Beladeliste from database: "
                         + exc.getMessage() + " caused by: "
                         + ChainedSQLExceptionWalker.getAllMessages(exc);
            logger.log(Level.SEVERE, loggingPrefix + "1022" + "|" + txt, exc);
            throw new DAOException(txt);
        } finally {
            if (connection != null) {
                dbAccess.closeConnection(connection);
            }
        }
    }

    /**
     * Creates the avis beladeliste.
     *
     * @param avisResult the avis result
     * @param fingerprint the fingerprint
     * @param includeJIS the include jis
     * @return the beladeliste
     * @throws SQLException the SQL exception
     */
    private Beladeliste createAvisBeladeliste(ResultSet avisResult,
                                              BeladelisteFingerprint fingerprint, boolean includeJIS)
            throws SQLException {
        BeladelisteImpl beladeListe = new BeladelisteImpl();

        boolean loaded = false;

        List sendungen = new LinkedList();
        beladeListe.setBeladung(sendungen);
        Sendung lastSendung = null;
        Lieferschein lastLieferschein = null;
        List lieferscheine = new LinkedList();
        List loadingUnits = new LinkedList();
        List subLoadingUnits = loadingUnits;
        List jisLoadingUnits = new ArrayList();
        if (includeJIS) {
            beladeListe.setPackstueckeJIS(jisLoadingUnits);
        }
        while (avisResult.next()) {
            if (!loaded) {
                // set parameters for charging list
                beladeListe.setSpediteurnummer(avisResult
                        .getString(COL_TRANSPORTPARTNERNUMMER));
                beladeListe.setKfzKennzeichen(avisResult
                        .getString(COL_KFZKENNZEICHEN));
            }
            Sendung currentSendung = buildSendung(avisResult);
            // update finger print info
            updateFingerPrint(avisResult, fingerprint);
            Lieferschein currentLieferschein = buildLieferschein(avisResult);
            if ((lastSendung == null) || (!lastSendung.equals(currentSendung))) {
                sendungen.add(currentSendung);
                lastSendung = currentSendung;
                lieferscheine = new LinkedList();
                lieferscheine.add(currentLieferschein);
                currentSendung.setLieferscheine(lieferscheine);
                loadingUnits = new LinkedList();
                subLoadingUnits = loadingUnits;
                currentLieferschein.setPackstuecke(loadingUnits);
                lastLieferschein = currentLieferschein;
            } else if ((lastLieferschein == null)
                       || (!lastLieferschein.equals(currentLieferschein))) {
                lieferscheine.add(currentLieferschein);
                loadingUnits = new LinkedList();
                subLoadingUnits = loadingUnits;
                currentLieferschein.setPackstuecke(loadingUnits);
                lastLieferschein = currentLieferschein;
            }
            List expandedUnits = buildPackstuecke(avisResult);
            if (includeJIS) {
                for (Iterator it = expandedUnits.iterator(); it.hasNext();) {
                    PackstueckPosition pp = (PackstueckPosition) it.next();
                    String label = pp.getLabelkennung();
                    if ((label != null) && (label.trim().length() == 0)) {
                        jisLoadingUnits.add(pp);
                    }
                }
            }
            if ((expandedUnits.size() == 1)
                && (((PackstueckPosition) expandedUnits.get(0)).isMaster())) {

                subLoadingUnits = new LinkedList();
                ((PackstueckPosition) expandedUnits.get(0))
                        .setPackstuecke(subLoadingUnits);
                loadingUnits.add(expandedUnits.get(0));
            } else {
                subLoadingUnits.addAll(expandedUnits);
            }
        }
        return beladeListe;
    }

    /**
     * Creates the avis loading list.
     *
     * @param avisResult the avis result
     * @return the list
     * @throws SQLException the SQL exception
     */
    private List<PackstueckPosition> createAvisLoadingList(ResultSet avisResult)
            throws SQLException {
        List<PackstueckPosition> loadingUnits = new ArrayList<PackstueckPosition>();

        boolean loaded = false;

        while (avisResult.next()) {
            // update finger print info
            // updateFingerPrint(avisResult, fingerprint);
            List<PackstueckPosition> expandedUnits = buildPackstuecke(avisResult);
            /*
             * if ((expandedUnits.size() == 1) &&
             * (((PackstueckPosition)expandedUnits.get(0)).isMaster())) {
             *
             * subLoadingUnits = new LinkedList();
             * ((PackstueckPosition)expandedUnits
             * .get(0)).setPackstuecke(subLoadingUnits);
             * loadingUnits.add(expandedUnits.get(0)); } else {
             * subLoadingUnits.addAll(expandedUnits); }
             */
            loadingUnits.addAll(expandedUnits);
        }
        return loadingUnits;
    }

    /**
     * Update finger print.
     *
     * @param avisResult the avis result
     * @param fingerprint the fingerprint
     * @throws SQLException the SQL exception
     */
    private static void updateFingerPrint(ResultSet avisResult,
                                          BeladelisteFingerprint fingerprint) throws SQLException {
        fingerprint.addLine(avisResult.getLong(COL_TID),
                avisResult.getLong(COL_S_ID));
    }

    /**
     * Builds the packstuecke.
     *
     * @param avisResult the avis result
     * @return the list
     * @throws SQLException the SQL exception
     */
    private static List<PackstueckPosition> buildPackstuecke(
            ResultSet avisResult) throws SQLException {
        List<PackstueckPosition> list = new LinkedList<PackstueckPosition>();
        PackstueckPosition pp = new PackstueckPositionImpl();
        pp.setLabelkennung(avisResult.getString(COL_LABELKENNUNG));
        String pnrVon = avisResult.getString(COL_PACKSTUECKNUMMERVON);
        String pnrBis = avisResult.getString(COL_PACKSTUECKNUMMERBIS);
        String anzPackmittel = avisResult.getString(COL_ANZAHLPACKMITTEL);
        int anz = -1;
        try {
            anz = Integer.parseInt(anzPackmittel);
        } catch (NumberFormatException e) {
            // TODO Error Handling
        }

        if ((!pp.isMaster()) && (!"".equals(pnrBis)) && (!"".equals(pnrVon))
            && (anzPackmittel != null) && (anz > 1)) {
            // check for compressed loading untis
            try {
                long from = Long.parseLong(pnrVon.trim());
                long to = Long.parseLong(pnrBis.trim());
                long num = Long.parseLong(anzPackmittel.trim());
                // TODO make max. configurable
                if ((to - from + 1 == num)
                    && (num <= DEFAULT_MAX_UNIT_COMPRESSION_COUNT)) {
                    // compression is OK
                    for (int i = 0; i < num; i++) {
                        PackstueckPosition epp = buildPackstueck(avisResult);
                        epp.setPackstuecknummer(formatNumberByTemplate(
                                pnrVon.trim(), from + i));
                        list.add(epp);
                    }
                }
            } catch (NumberFormatException nfex) {
                // TODO error handling
                nfex.printStackTrace();
                list.add(buildPackstueck(avisResult));
            } catch (NullPointerException npex) {
                npex.printStackTrace();
                list.add(buildPackstueck(avisResult));
            }
        } else {
            if (!pp.isMaster() || (pp.isMaster() && anz == 1)) {
                // some items with label G have anz == 0 and must not be added
                // to the list
                list.add(buildPackstueck(avisResult));
            }
        }
        return list;
    }

    /**
     * Creates a string representation from a number that uses the same number
     * of leading zeros than the given template number representation. For
     * example, if template is "00010" and number is 1234 then the result is
     * "01234".
     *
     * @param templateLowerBound
     * an example template representation whose number value must be
     * less or equal to <code>number</code>.
     * @param number
     * the long number to format.
     * @return the formatted string representation of <code>number</code>.
     */
    private static String formatNumberByTemplate(String templateLowerBound,
                                                 long number) {
        boolean isDebugEnabled = logger.isLoggable(Level.FINE);

        if (isDebugEnabled) {
            logger.log(Level.FINE, loggingPrefix + "formattingNumerByTemplate");
            logger.log(Level.FINE, loggingPrefix + "templateLowerBound: "
                                   + templateLowerBound);
            logger.log(Level.FINE, loggingPrefix + "number: " + number);
        }

        String result = "" + number;
        if (templateLowerBound.startsWith("0")
            && templateLowerBound.length() > result.length()) {

            if (isDebugEnabled) {
                logger.log(Level.FINE, loggingPrefix + "transforming result...");
                logger.log(Level.FINE,
                        loggingPrefix + "templateLowerBound.length():"
                        + templateLowerBound.length());
                logger.log(Level.FINE, loggingPrefix + "result.length():"
                                       + result.length());
                logger.log(
                        Level.FINE,
                        loggingPrefix
                        + (templateLowerBound.length() - result
                                .length()));
            }

            result = templateLowerBound.substring(0,
                    templateLowerBound.length() - result.length())
                     + result;
        }

        if (isDebugEnabled) {
            logger.log(Level.FINE, loggingPrefix + "Result: " + result);
        }

        return result;
    }

    /**
     * Builds the packstueck.
     *
     * @param avisResult the avis result
     * @return the packstueck position
     * @throws SQLException the SQL exception
     */
    private static PackstueckPosition buildPackstueck(ResultSet avisResult)
            throws SQLException {
        PackstueckPosition pp = new PackstueckPositionImpl();
        pp.setMasterpackstuecknummer(avisResult
                .getString(COL_MASTERPACKSTUECKNUMMER));
        pp.setPackstuecknummer(avisResult.getString(COL_PACKSTUECKNUMMER));
        pp.setAbladestelle(avisResult.getString(COL_ABLADESTELLE));
        pp.setAnzahlPackmittel(avisResult.getString(COL_ANZAHLPACKMITTEL));
        pp.setChargennummer(avisResult.getString(COL_CHARGENNUMMER));
        pp.setDublette(avisResult.getShort(COL_DUBLETTE));
        pp.setEigentumskennung(avisResult.getString(COL_EIGENTUMSKENNUNG));
        pp.setGeometrieBreite(avisResult.getString(COL_GEOMETRIEBREITE));
        pp.setGeometrieFuellmenge(avisResult.getString(COL_GEOMETRIEFUELLMENGE));
        pp.setGeometrieGewicht(avisResult.getString(COL_GEOMETRIEGEWICHT));
        pp.setGeometrieHoehe(avisResult.getString(COL_GEOMETRIEHOEHE));
        pp.setGeometrieLaenge(avisResult.getString(COL_GEOMETRIELAENGE));
        pp.setGeometrieStapelfaktor(avisResult.getString(COL_STAPELFAKTOR));
        pp.setId(avisResult.getLong(COL_PP_ID));
        pp.setKennzeichenUlUel(avisResult.getString(COL_KENNZEICHENULUEL));
        pp.setLabelkennung(avisResult.getString(COL_LABELKENNUNG));
        pp.setLadungstraegerpositionsNr(avisResult
                .getString(COL_LADUNGSTRAEGERPOSITIONSNR));
        pp.setLagerabrufnummer(avisResult.getString(COL_LAGERABRUFNUMMER));
        pp.setLieferantennummer(avisResult.getString(COL_HERSTELLERNUMMER));
        pp.setLieferscheinId(avisResult.getLong(COL_LS_ID));
        pp.setLieferscheinpackstueckRef(avisResult
                .getString(COL_LIEFERSCHEINPACKSTUECKREF));
        pp.setLieferscheinpositionId(avisResult.getLong(COL_LP_ID));
        if (!pp.isMaster()) {
            pp.setLieferscheinpositionsnummer(avisResult
                    .getString(COL_LIEFERSCHEINPOSITIONSNUMMER));
        }
        // duplicate entry
        // pp.setMasterpackstuecknummer(avisResult.getString(COL_MASTERPACKSTUECKNUMMER));
        pp.setPackmittelnummer(avisResult.getString(COL_PACKMITTELNUMMER));
        pp.setPackmittelnummerLieferant(avisResult
                .getString(COL_PACKMITTELNUMMERLIEFERANT));
        pp.setPositionsnummerLieferschein(avisResult
                .getString(COL_POSITIONSNUMMERLIEFERSCHEIN));
        pp.setSendungId(avisResult.getLong(COL_S_ID));
        pp.setStatus(avisResult.getString(COL_STATUS));
        pp.setStatusInBeladeliste(PackstueckPosition.STATUS_ROT);
        pp.setVerpackungskennung(avisResult.getString(COL_VERPACKUNGSKENNUNG));
        return pp;
    }

    /**
     * Builds the lieferschein.
     *
     * @param avisResult the avis result
     * @return the lieferschein
     * @throws SQLException the SQL exception
     */
    private static Lieferschein buildLieferschein(ResultSet avisResult)
            throws SQLException {
        Lieferschein ls = new LieferscheinImpl();
        ((LieferscheinImpl) ls).setLieferscheinnummer(avisResult.getString(COL_LIEFERSCHEINNUMMER));
        ls.setHerstellernummer(avisResult.getString(COL_HERSTELLERNUMMER));
        return ls;
    }

    /**
     * Builds the sendung.
     *
     * @param avisResult the avis result
     * @return the sendung
     * @throws SQLException the SQL exception
     */
    private static Sendung buildSendung(ResultSet avisResult)
            throws SQLException {
        Sendung s = new SendungImpl();
        ((SendungImpl) s).setDatenerstellernummer(avisResult
                .getString(COL_DATENERSTELLERNUMMER));
        ((SendungImpl) s).setSendungsladungsbezugsnummer(avisResult
                .getString(COL_SENDUNGSLADUNGSBEZUGSNUMMER));
        return s;
    }

    /* (non-Javadoc)
     * @see com.tsystems.dqm.tmgmt.beladeliste.dao.BeladelisteDAO#storeBeladeliste(com.tsystems.dqm.tmgmt.beladeliste.interfaces.Beladeliste)
     */
    public boolean storeBeladeliste(Beladeliste beladeliste)
            throws DAOException {
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, loggingPrefix + "1023" + "|"
                                   + "starting storeBeladeliste with " + beladeliste);
        }
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null,
                null);
        dbAccess.setMaxNumberOfRows(0);
        dbAccess.addUnresolvedFilterListener(this);
        dbAccess.addUnresolvedParameterListener(this);

        Connection connection = null;
        try {
            connection = dbAccess
                    .openConnection(Connection.TRANSACTION_SERIALIZABLE);

            Map p = new HashMap(5);
            p.put(PARAM_WERKNUMMER, beladeliste.getWerknummer());
            p.put(PARAM_BORDERONUMMER, beladeliste.getBorderoNummer());
            p.put(PARAM_BORDERONUMMER_9, beladeliste.getBorderoNummer()
                                                 .substring(0, 8) + "_");
            p.put(PARAM_VERSION, Integer.valueOf(beladeliste.getVersion()));

            SQLStatement stmt = dbAccess.createPreparedStatement(
                    SELECT_BELADELISTE, connection, p);
            ResultSet versionResult = dbAccess.executePreparedStatement(stmt
                    .getPreparedStatement());

            int version = -1;
            if (versionResult.next()) {
                version = versionResult.getInt(1);
                stmt.getPreparedStatement().close();
            } else {
                stmt.getPreparedStatement().close();
                return true;
            }
            stmt = dbAccess.createPreparedStatement(SELECT_PACKSTUECK_AVIS,
                    connection, p);
            ResultSet avisResult = dbAccess.executePreparedStatement(stmt
                    .getPreparedStatement());

            BeladelisteFingerprint fpCurrent = new BeladelisteFingerprint();

            createAvisBeladeliste(avisResult, fpCurrent, false);
            stmt.getPreparedStatement().close();

            stmt = dbAccess.createPreparedStatement(SELECT_FINGERPRINT,
                    connection, p);
            ResultSet fpResult = dbAccess.executePreparedStatement(stmt
                    .getPreparedStatement());
            BeladelisteFingerprint fp = buildFingerprint(fpResult);
            stmt.getPreparedStatement().close();

            // delete existing scanned loading units
            stmt = dbAccess.createPreparedStatement(DELETE_SCANNED, connection,
                    p);
            dbAccess.executePreparedStatement(stmt.getPreparedStatement());
            stmt.getPreparedStatement().close();
            stmt = null;

            // actually write scanned loading units
            List scannedLoadingUnits = extractScannedLoadingUntis(beladeliste);

            int batchCount = 1;
            for (Iterator it = scannedLoadingUnits.iterator(); it.hasNext(); batchCount++) {
                if ((stmt != null) && (batchCount % BATCH_COMMIT_COUNT == 0)) {
                    stmt.getPreparedStatement().executeBatch();
                }
                PackstueckPosition pp = (PackstueckPosition) it.next();

                String lieferantennummer = pp.getLieferantennummer();
                String packstuecknummer = pp.getPackstuecknummer();
                Date scanzeitpunkt = pp.getScanzeitpunkt();

                String params = "";
                if (lieferantennummer == null || packstuecknummer == null
                    || scanzeitpunkt == null) {
                    params = "Parameters: Lieferantennummer=[ "
                             + lieferantennummer + "] // Packstuecknummer = [ "
                             + packstuecknummer + "] //" + " Scanzeitpunkt = ["
                             + scanzeitpunkt + "]";
                }

                if (lieferantennummer == null) {
                    throw new DAOException(
                            "Missing Parameter! Lieferantennummer"
                            + " in PackstueckPostion must not be null! "
                            + params);
                }
                if (packstuecknummer == null) {
                    throw new DAOException(
                            "Missing Parameter! Packstuecknummer "
                            + "in PackstueckPostion must not be null! "
                            + params);
                }
                if (scanzeitpunkt == null) {
                    throw new DAOException("Missing Parameter! Scanzeitpunkt "
                                           + "in PackstueckPostion must not be null! "
                                           + params);
                }
                p.put(PARAM_LIEFERANTENNUMMER, lieferantennummer);
                p.put(PARAM_PACKSTUECKNUMMER, packstuecknummer);
                p.put(PARAM_SCANZEITPUNKT, scanzeitpunkt);

                if (stmt == null) {
                    stmt = dbAccess.createPreparedStatement(INSERT_SCANNED,
                            connection, p);
                } else {
                    dbAccess.fillPreparedStatement(stmt, p);
                }
                stmt.getPreparedStatement().addBatch();
            }
            if (stmt != null) {
                stmt.getPreparedStatement().executeBatch();
                stmt.getPreparedStatement().close();
            }

            return ((version != beladeliste.getVersion()) || !fp
                    .equals(fpCurrent));
        } catch (SQLException exc) {
            String txt = "Could not store Beladeliste to database: "
                         + exc.getMessage() + " caused by: "
                         + ChainedSQLExceptionWalker.getAllMessages(exc);
            logger.log(Level.SEVERE, loggingPrefix + "1024" + "|" + txt, exc);
            throw new DAOException(txt);
        } finally {
            if (connection != null) {
                dbAccess.closeConnection(connection);
            }
        }
    }

    /**
     * Extract scanned loading untis.
     *
     * @param beladeliste the beladeliste
     * @return the list
     */
    private static List extractScannedLoadingUntis(Beladeliste beladeliste) {
        return beladeliste.getScannedPackstuecke();
    }

    /**
     * Builds the fingerprint.
     *
     * @param fpResult the fp result
     * @return the beladeliste fingerprint
     * @throws SQLException the SQL exception
     */
    private BeladelisteFingerprint buildFingerprint(ResultSet fpResult)
            throws SQLException {
        BeladelisteFingerprint fp = new BeladelisteFingerprint();
        while (fpResult.next()) {
            fp.addLine(fpResult.getLong(1), fpResult.getLong(2));
        }
        return fp;
    }

    /*
     * (Kein Javadoc)
     *
     * @see com.tsystems.dqm.tmgmt.beladeliste.dao.BeladelistePrimaryKeyBuilder#
     * buildPrimaryKey(java.lang.String, java.lang.String)
     */
    /* (non-Javadoc)
     * @see com.tsystems.dqm.tmgmt.beladeliste.dao.BeladelistePrimaryKeyBuilder#buildPrimaryKey(java.lang.String, java.lang.String)
     */
    public BeladelistePrimaryKey buildPrimaryKey(String borderonummer,
                                                 String werknummer) {
        BeladelistePrimaryKey primaryKey = new BeladelistePrimaryKeyImpl();
        primaryKey.setBorderoNummer(borderonummer);
        primaryKey.setWerknummer(werknummer);
        return primaryKey;
    }

    /*
     * (Kein Javadoc)
     *
     * @see com.tsystems.dqm.tmgmt.beladeliste.dao.BeladelistePrimaryKeyBuilder#
     * buildNewPrimaryKey(java.lang.String, java.lang.String)
     */
    /* (non-Javadoc)
     * @see com.tsystems.dqm.tmgmt.beladeliste.dao.BeladelistePrimaryKeyBuilder#buildNewPrimaryKey(java.lang.String, java.lang.String)
     */
    public BeladelistePrimaryKey buildNewPrimaryKey(String borderonummer,
                                                    String werknummer) {
        return buildPrimaryKey(borderonummer, werknummer);
    }

    /*
     * (Kein Javadoc)
     *
     * @see
     * com.tsystems.dqm.tmgmt.beladeliste.dao.BeladelisteDAO#getSachnummern(
     * java.util.List)
     */
    /* (non-Javadoc)
     * @see com.tsystems.dqm.tmgmt.beladeliste.dao.BeladelisteDAO#getLieferscheinPositionen(java.util.List)
     */
    public Map getLieferscheinPositionen(List packstueckPositionIDs)
            throws DAOException {
        initConfigLoader();
        DefaultDBAccess dbAccess = new DefaultDBAccess(config, CONTEXT, null,
                null);
        dbAccess.setMaxNumberOfRows(0);
        dbAccess.addUnresolvedFilterListener(this);
        dbAccess.addUnresolvedParameterListener(this);

        Connection connection = null;
        try {
            connection = dbAccess
                    .openConnection(Connection.TRANSACTION_SERIALIZABLE);

            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, loggingPrefix
                                       + "Getting 'LieferscheinPosition' as list");
            }

            dbAccess.setFilter(FILTER_IDS, ConstantList.createConstantList(
                    null, packstueckPositionIDs, true));

            SQLStatement stmt = dbAccess.createPreparedStatement(
                    SELECT_SACHNUMMERN, connection, new HashMap());
            ResultSet result = dbAccess.executePreparedStatement(stmt
                    .getPreparedStatement());

            Map map = new HashMap();
            while (result.next()) {
                LieferscheinPosition l = new LieferscheinPositionImpl();

                l.setId(result.getLong(COL_ID));
                l.setSachnummerKunde(result.getString(COL_SNR));
                l.setMengeneinheit1(result.getString(COL_UNIT));
                l.setPackstueckPositionId(result.getLong(COL_PID));

                map.put(Long.valueOf(l.getPackstueckPositionId()), l);
            }

            return map;

        } catch (SQLException exc) {
            String txt = "Could not load SNRs from database: "
                         + exc.getMessage() + " caused by: "
                         + ChainedSQLExceptionWalker.getAllMessages(exc);
            logger.log(Level.SEVERE, loggingPrefix + "1025" + "|" + txt, exc);
            throw new DAOException(txt);
        } finally {
            if (connection != null) {
                dbAccess.closeConnection(connection);
            }
        }
    }

}