package org.infinispan.cdc.internal.url;

import org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor;

/**
 * A JDBC URL for an Oracle database.
 *
 * <p>
 * The JDBC URL for an Oracle database has some unique formats. The current implementation only handles URLs in the
 * EZConnect format, with the general format of:
 *
 * <pre>
 * <code>jdbc:oracle:thin:@[[protocol:]//]host1[,host2,host3][:port1][,host4:port2] [/service_name][:server_mode][/instance_name][?connection properties]</code>
 * </pre>
 *
 * With this variability, the current implementation does not handle all arguments, and focuses only on a few of them.
 * We need to identify the host name, port, and any schema name to connect to. Observe this URL allows for either an SID
 * or the service name to connect.
 * </p>
 *
 * <p>
 * <b>Warning:</b> This implementation does not handle URLs in the TNS format.
 * </p>
 *
 * @since 15.2
 * @author José Bolina
 * @see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleDriver.html">Oracle Driver documentation</a>
 */
final class OracleDatabaseURL implements DatabaseURL {

   private final String host;
   private final String port;
   private final String schema;

   OracleDatabaseURL(String url) {
      String[] parts = hostPortSchema(url);
      this.host = parts[0];
      this.port = parts[1];
      this.schema = parts[2];
   }

   @Override
   public String host() {
      return host;
   }

   @Override
   public String port() {
      return port;
   }

   @Override
   public String database() {
      return schema;
   }

   @Override
   public DatabaseVendor vendor() {
      return DatabaseVendor.ORACLE;
   }

   private static String[] hostPortSchema(String url) {
      String value = url;

      int at = value.indexOf('@');
      if (at < 0)
         throw new IllegalArgumentException("Could not parse JDBC URL: " + url);

      value = value.substring(at + 1);

      if (value.startsWith("//"))
         value = value.substring(2);

      int slash = value.indexOf('/');
      if (slash > 0) {
         String[] splitSlash = value.split("/");
         String[] hostAndPort = splitSlash[0].split(":");
         return new String[] { hostAndPort[0], hostAndPort[1], splitSlash[1] };
      }

      return value.split(":");
   }

   @Override
   public String toString() {
      return String.format("jdbc:oracle:thin:@%s:%s:%s", host, port, schema);
   }
}
