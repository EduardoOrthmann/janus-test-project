package com.tsystems.dqm.tmgmt.tourenverwaltung.dao;

import com.tsystems.dao.xml.DAOException;
import com.tsystems.dqm.tmgmt.tourenverwaltung.interfaces.Tour;
import java.sql.Connection;
import java.util.Date;
import java.util.List;

/**
 * Placeholder for the Tour DAO (Data Access Object) interface.
 * It defines the contract for all tour-related database operations.
 */
public interface TourDAO {

    Tour getTour(long id) throws DAOException;

    List getTourenByBorderonummer(String borderonummer, String werknummer) throws DAOException;

    List getTourenByStatus(String borderonummer, String werknummer, int status) throws DAOException;

    List getTourenByTimeframeGroup(String timeframeGroup, String plant) throws DAOException;

    Tour saveTour(Tour tour) throws DAOException;

    Date[] getZeitfensterFuerTour(long tourId) throws DAOException;

    List getAlleLokationenFuerWerk(String werknummer) throws DAOException;

    List getTourenByBorderovorsatz(String borderovorsatz, String werknummer) throws DAOException;

    List getTourenByBorderovorsatzAndStatus(String borderovorsatz, String werknummer, int status) throws DAOException;

    List getBorderoNummernByBorderoVorsatz(String borderovorsatz, String werknummer) throws DAOException;

    String getNewBorderoNummer(String werknummer) throws DAOException;

    long[] getBorderonummerGenerationInterval(String werknummer) throws DAOException;

    int deleteTours(Date deleteBefore) throws DAOException;
}