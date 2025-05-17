package org.infinispan.client.hotrod.impl;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.logging.Log;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class HotRodURI {

   public static final String[] EMPTY_STRING_ARRAY = new String[0];

   public static HotRodURI create(String uriString) {
      return create(URI.create(uriString));
   }

   public static HotRodURI create(URI uri) {
      if (!"hotrod".equals(uri.getScheme()) && !"hotrods".equals(uri.getScheme())) {
         throw Log.HOTROD.notaHotRodURI(uri.toString());
      }
      final List<InetSocketAddress> addresses;
      final String[] userInfo;
      if (uri.getHost() != null) {
         addresses = Collections.singletonList(InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort() < 0 ? ConfigurationProperties.DEFAULT_HOTROD_PORT : uri.getPort()));
         userInfo = uri.getUserInfo() != null ? uri.getUserInfo().split(":") : EMPTY_STRING_ARRAY;
      } else {
         // We need to handle this by hand
         String authority = uri.getAuthority();
         final int at = authority.indexOf('@');
         userInfo = at < 0 ? EMPTY_STRING_ARRAY : authority.substring(0, at).split(":");
         String[] hosts = at < 0 ? authority.split(",") : authority.substring(at + 1).split(",");
         addresses = new ArrayList<>(hosts.length);
         for (String host : hosts) {
            int colon = host.lastIndexOf(':');
            addresses.add(InetSocketAddress.createUnresolved(colon < 0 ? host : host.substring(0, colon), colon < 0 ? ConfigurationProperties.DEFAULT_HOTROD_PORT : Integer.parseInt(host.substring(colon + 1))));
         }
      }
      Properties properties = new Properties();
      if (uri.getQuery() != null) {
         String[] parts = uri.getQuery().split("&");
         for (String part : parts) {
            int eq = part.indexOf('=');
            if (eq < 0) {
               throw Log.HOTROD.invalidPropertyFormat(part);
            } else {
               properties.setProperty(ConfigurationProperties.ICH + part.substring(0, eq), part.substring(eq + 1));
            }
         }
      }
      return new HotRodURI(addresses, "hotrods".equals(uri.getScheme()), userInfo.length > 0 ? userInfo[0] : null, userInfo.length > 1 ? userInfo[1] : null, properties);

   }

   private final List<InetSocketAddress> addresses;
   private final boolean ssl;
   private final String username;
   private final String password;
   private final Properties properties;


   private HotRodURI(List<InetSocketAddress> addresses, boolean ssl, String username, String password, Properties properties) {
      this.addresses = addresses;
      this.ssl = ssl;
      this.username = username;
      this.password = password;
      this.properties = properties;
   }

   public List<InetSocketAddress> getAddresses() {
      return addresses;
   }

   public boolean isSsl() {
      return ssl;
   }

   public String getUsername() {
      return username;
   }

   public String getPassword() {
      return password;
   }

   public Properties getProperties() {
      return properties;
   }

   public ConfigurationBuilder toConfigurationBuilder() {
      return toConfigurationBuilder(new ConfigurationBuilder());
   }

   public ConfigurationBuilder toConfigurationBuilder(ConfigurationBuilder builder) {
      for (InetSocketAddress address : addresses) {
         builder.addServer().host(address.getHostString()).port(address.getPort());
      }
      if (ssl) {
         builder.security().ssl().enable();
      }
      if (username != null) {
         builder.security().authentication().username(username);
      }
      if (password != null) {
         builder.security().authentication().password(password);
      }
      builder.withProperties(properties);
      return builder;
   }

   @Override
   public String toString() {
      return toString(false);
   }

   public String toString(boolean withCredentials) {
      StringBuilder sb = new StringBuilder();
      if (ssl) {
         sb.append("hotrods://");
      } else {
         sb.append("hotrod://");
      }
      if (withCredentials) {
         sb.append(username);
         sb.append(':');
         sb.append(password);
         sb.append('@');
      }
      for (int i = 0; i < addresses.size(); i++) {
         if (i > 0) {
            sb.append(',');
         }
         InetSocketAddress address = addresses.get(i);
         sb.append(address.getHostString());
         if (address.getPort() != ConfigurationProperties.DEFAULT_HOTROD_PORT) {
            sb.append(':');
            sb.append(address.getPort());
         }
      }
      if (!properties.isEmpty()) {
         sb.append('?');
         for (Map.Entry<Object, Object> property : properties.entrySet()) {
            String key = property.getKey().toString();
            if (key.startsWith(ConfigurationProperties.ICH)) {
               sb.append(key.substring(ConfigurationProperties.ICH.length()));
            } else {
               sb.append(key);
            }
            sb.append('=');
            sb.append(property.getValue());

         }
      }
      return sb.toString();
   }
}
