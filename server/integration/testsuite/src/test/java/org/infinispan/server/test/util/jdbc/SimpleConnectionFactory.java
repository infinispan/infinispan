package org.infinispan.server.test.util.jdbc;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
 * @since 7.0
 */
public class SimpleConnectionFactory {
    private static final Log log = LogFactory.getLog(SimpleConnectionFactory.class);

    private final String connectionUrl;
    private final String userName;
    private final String password;


    public SimpleConnectionFactory(String connectionUrl, String userName, String password) {
        this.connectionUrl = connectionUrl;
        this.userName = userName;
        this.password = password;
    }

    public void start(String driverClass) {
        loadDriver(driverClass);
    }

    public Connection getConnection() {
        try {
            Connection connection = DriverManager.getConnection(connectionUrl, userName, password);
            if (connection == null)
                throw new RuntimeException("Received null connection from the DriverManager!");
            return connection;
        } catch (SQLException e) {
            throw new RuntimeException("Could not obtain a new connection", e);
        }
    }

    public void releaseConnection(Connection conn) {
        try {
            conn.close();
        } catch (SQLException e) {
            log.warnf(e, "Error closing connection %s", conn);
        }
    }

    private void loadDriver(String driverClass) {
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            log.errorf(e, "Driver class %s not found in classpath", driverClass);
        }
    }

    @Override
    public String toString() {
        return "SimpleConnectionFactory{" + "connectionUrl='" + connectionUrl + '\'' + ", userName='" + userName + '\'' + "} "
                + super.toString();
    }
}