package com.tsystems.dao.xml.db;

import com.tsystems.dao.xml.config.ConfigLoader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import javax.swing.table.DefaultTableModel;

public class DefaultDBAccess extends AbstractDBAccess {
    public DefaultDBAccess(ConfigLoader config, String context, Object p1, Object p2) {}
    public void setMaxNumberOfRows(int rows) {}
    public void addUnresolvedFilterListener(Object listener) {}
    public void addUnresolvedParameterListener(Object listener) {}
    public Connection openConnection(int transactionIsolation) throws SQLException { return null; }
    public void closeConnection(Connection conn) {}
    public SQLStatement createPreparedStatement(String queryName, Connection conn, Map params) { return new SQLStatement(); }
    public java.sql.ResultSet executePreparedStatement(java.sql.PreparedStatement ps) throws com.tsystems.dao.xml.DAOException { return null; }
    public void execute(String queryName, Connection conn, Map params) {}
    public DefaultTableModel resultSet2tableModel(java.sql.ResultSet rs) { return new DefaultTableModel(); }
    public void setFilter(String name, Object value) {}
    public void fillPreparedStatement(SQLStatement stmt, Map params) {}
}