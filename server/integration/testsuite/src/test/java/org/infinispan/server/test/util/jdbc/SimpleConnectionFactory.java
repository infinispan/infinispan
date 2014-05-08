package org.infinispan.server.test.util.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
 * @since 7.0
 */
public class SimpleConnectionFactory {

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
            e.printStackTrace();
        }
    }

    private void loadDriver(String driverClass) {
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            System.out.println("Driver class not found in classpath");
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "SimpleConnectionFactory{" + "connectionUrl='" + connectionUrl + '\'' + ", userName='" + userName + '\'' + "} "
                + super.toString();
    }
}