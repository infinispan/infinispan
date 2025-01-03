package org.infinispan.cdc.internal.url;

import static org.infinispan.cdc.internal.url.DatabaseConstants.SQLSERVER_DEFAULT_PORT;

import org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor;

/**
 * A JDBC URL for a SQL Server database.
 *
 * <p>
 * The JDBC format has some unique features. The general format for the JDBC URL to a SQL Server is:
 *
 * <pre>
 * <code>jdbc:sqlserver://[serverName[\instanceName][:portNumber]][;property=value[;property=value]]</code>
 * </pre>
 *
 * Observe that most of the URL part is optional. If they are not provided they must be present as properties. The
 * `<code>serverName</code>` must be in the URL or as a property. The default port utilized is `<code>1433</code>`.
 * </p>
 *
 * @since 15.2
 * @author José Bolina
 * @see <a href="https://learn.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url?view=sql-server-ver16">SQL Server documentation.</a>
 */
final class SqlServerDatabaseURL implements DatabaseURL {

   private final String host;
   private final String port;
   private final String schema;

   SqlServerDatabaseURL(String url) {
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
      return DatabaseVendor.SQL_SERVER;
   }

   private static String[] hostPortSchema(String url) {
      if (!url.startsWith("jdbc:"))
         throw new IllegalArgumentException("Not a JDBC URL");

      String value = url;
      int beginning = value.indexOf("//");
      if (beginning < 0)
         throw new IllegalArgumentException("Unable to parse JDBC URL: " + url);

      value = value.substring(beginning + 2);
      int semicolon = value.indexOf(';');
      if (semicolon < 0)
         throw new IllegalArgumentException("Unable to parse JDBC URL: " + url);

      String hostAndPort = value.substring(0, semicolon);
      String host = null;
      String port = SQLSERVER_DEFAULT_PORT;
      String database = "";

      value = value.substring(semicolon + 1);
      String[] arguments = value.split(";");
      for (String argument : arguments) {
         String[] keyValue = argument.split("=");
         switch (keyValue[0].toLowerCase()) {
            case "databasename", "database" -> database = keyValue[1];
            case "portnumber", "port" -> port = keyValue[1];
            case "servername" -> host = keyValue[1];
         }
      }

      // TODO: Not handling URL with an instance name.
      String[] split = hostAndPort.split(":");
      if (split.length > 0) {
         host = split[0].isBlank() ? host : split[0];

         if (split.length > 1)
            port = split[1];
      }

      if (host == null)
         throw new IllegalArgumentException("Host name not provided in URL: " + url);

      return new String[] { host, port, database };
   }

   @Override
   public String toString() {
      return String.format("jdbc:sqlserver://%s:%s;databaseName=%s;", host, port, schema);
   }
}
