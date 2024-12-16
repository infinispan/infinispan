package org.infinispan.cdc.internal.url;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.logging.Logger;

import org.infinispan.commons.util.Util;

/**
 * Base class to implement the parsing of URLs for specific vendors.
 *
 * <p>
 * The parser utilizes the {@link Driver} capabilities to extract required properties from the JDBC URL. This approach
 * also validates the URL is valid and that there is a driver available. This removed the need for manually parsing
 * a URL for every vendor.
 * </p>
 *
 * @author José Bolina
 * @since 16.0
 */
abstract class BaseJdbcURL implements DatabaseURL {

   private final Properties properties;

   /**
    * Initialize the JDBC URL parser.
    *
    * <p>
    * The constructor will parse the given URL and extract the necessary parameters.
    * </p>
    *
    * @param jdbcUrl The JDBC URL to parse.
    * @throws SQLException If failed to identify a {@link Driver} or extract the properties from the URL.
    * @throws NullPointerException If the given URL is <code>null</code>.
    */
   protected BaseJdbcURL(String jdbcUrl) throws SQLException {
      Objects.requireNonNull(jdbcUrl, "URL can not be null");
      this.properties = retrieveProperties(jdbcUrl);
   }

   /**
    * Non-null string with the name of the property to retrieve the database host to connect.
    *
    * @return Host property name.
    */
   protected abstract String hostProperty();

   /**
    * Non-null string with the name of the property to retrieve the database port to connect.
    *
    * @return Port property name.
    */
   protected abstract String portProperty();

   /**
    * @return Schema property name.
    */
   protected abstract String schemaProperty();

   /**
    * @return Database name property.
    */
   protected abstract String databaseNameProperty();

   @Override
   public final String host() {
      return properties.getProperty(hostProperty());
   }

   @Override
   public final String port() {
      return properties.getProperty(portProperty());
   }

   @Override
   public final String schema() {
      String prop = schemaProperty();
      return prop == null ? null : properties.getProperty(prop);
   }

   @Override
   public final String databaseName() {
      String prop = databaseNameProperty();
      return prop == null ? null : properties.getProperty(prop);
   }

   static Properties retrieveProperties(String jdbcUrl) throws SQLException {
      Properties properties = new Properties();
      Driver driver = ShimDriverManager.getDriver(jdbcUrl, Util.getClassLoaders(null));
      for (DriverPropertyInfo property : driver.getPropertyInfo(jdbcUrl, System.getProperties())) {
         properties.put(property.name, property.value == null ? "" : property.value);
      }

      return properties;
   }

   /**
    * Wrapper around the {@link DriverManager}.
    *
    * <p>
    * The original {@link DriverManager#getDriver(String)} is caller sensitive, and does not allow a custom
    * {@link ClassLoader} as argument to load the {@link Driver}. This causes the method to fail with a
    * {@link SQLException}, because even though the appropriate driver is found, a check with the class loader is performed.
    * </p>
    *
    * @since 16.0
    */
   private static final class ShimDriverManager {

      /**
       * Attempts to locate the {@link Driver} that accepts the given URL.
       *
       * <p>
       * This method accept custom {@link ClassLoader} to load the {@link Driver}. First, it will delegate the call to
       * {@link DriverManager#getDriver(String)}, and if it fails, it will proceed to load the driver through the
       * {@link ServiceLoader#load(Class, ClassLoader)} interface.
       * </p>
       *
       * @param url a database URL of the form
       *      *     <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>
       * @param cls the class loaders to load the {@link Driver} instance.
       * @return A {@link Driver} capable of understanding the given URL.
       * @throws SQLException If a {@link Driver} is not found.
       */
      public static Driver getDriver(String url, ClassLoader[] cls) throws SQLException {
         try {
            return DriverManager.getDriver(url);
         } catch (SQLException ignore) { }

         for (ClassLoader loader : cls) {
            Optional<ServiceLoader.Provider<Driver>> o = ServiceLoader.load(java.sql.Driver.class, loader).stream()
                  .filter(d -> acceptUrl(d.get(), url))
                  .findFirst();

            if (o.isEmpty()) continue;

            Driver driver = o.map(ServiceLoader.Provider::get).orElseThrow();
            // Register the driver again so subsequent invocations should succeed without using the ServiceLoader.
            DriverManager.registerDriver(new Driver() {
               @Override
               public Connection connect(String url, Properties info) throws SQLException {
                  return driver.connect(url, info);
               }

               @Override
               public boolean acceptsURL(String url) throws SQLException {
                  return driver.acceptsURL(url);
               }

               @Override
               public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
                  return driver.getPropertyInfo(url, info);
               }

               @Override
               public int getMajorVersion() {
                  return driver.getMajorVersion();
               }

               @Override
               public int getMinorVersion() {
                  return driver.getMinorVersion();
               }

               @Override
               public boolean jdbcCompliant() {
                  return driver.jdbcCompliant();
               }

               @Override
               public Logger getParentLogger() throws SQLFeatureNotSupportedException {
                  return driver.getParentLogger();
               }
            });
            return driver;
         }

         throw new SQLException("No suitable driver", "08001");
      }

      private static boolean acceptUrl(Driver driver, String url) {
         try {
            return driver.acceptsURL(url);
         } catch (SQLException e) {
            return false;
         }
      }
   }
}
