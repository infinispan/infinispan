package org.infinispan.cdc.internal.url;

import static org.infinispan.cdc.internal.url.DatabaseConstants.DATABASE_DEFAULT_HOST;

import java.net.URI;
import java.util.Objects;

import org.infinispan.cdc.internal.configuration.vendor.DatabaseVendor;

/**
 * The default implementation backed by a {@link URI} instance.
 *
 * <p>
 * The default implementation validates it is a JDBC URL. The string must follow a format similar to the RFC accepted
 * by {@link URI#create(String)} method. A format similar to:
 *
 * <pre>
 * <code>jdbc:[vendor]://[host][:port][/database][&parameters=values]</code>
 * </pre>
 *
 * The string must start with `<code>jdbc:</code>` and the `<code>[vendor]</code>` parameter is required.
 * </p>
 *
 * @author José Bolina
 * @since 15.2
 * @see URI#create(String)
 */
final class DefaultDatabaseURL implements DatabaseURL {

   private final String host;
   private final String port;
   private final String schema;
   private final DatabaseVendor vendor;

   DefaultDatabaseURL(String url, String port, DatabaseVendor vendor) {
      this.vendor = vendor;

      URI converted = transform(url, port);
      this.host = converted.getHost() == null ? DATABASE_DEFAULT_HOST : converted.getHost();
      this.port = converted.getPort() < 0 ? port: String.valueOf(converted.getPort());
      this.schema = getDatabaseName(converted.getPath());
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
      return vendor;
   }

   private static URI transform(String url, String port) {
      Objects.requireNonNull(url, "Database URL can not be null!");

      if (!url.startsWith("jdbc:"))
         throw new IllegalArgumentException("Not a JDBC URL");

      int begin = url.indexOf(":/");
      if (begin < 0)
         throw new IllegalStateException("Not found begin of URL: " + url);

      // A URL that contains only which database, without any other information:
      // jdbc:postgres:/
      if (url.substring(begin + 2).isEmpty())
         return URI.create(url.substring(5) + "/:" + port);

      // Might fail depending on the MySQL protocol format. For instance, `jdbc:mysql:replication`, might not parse properly.
      return URI.create(url.substring(5));
   }

   private static String getDatabaseName(String value) {
      if (!value.isEmpty() && value.charAt(0) == '/')
         return value.substring(1);
      return value;
   }

   @Override
   public String toString() {
      return String.format("jdbc:%s://%s:%s/%s", vendor.name(), host, port, schema);
   }
}
