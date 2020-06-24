package org.infinispan.client.rest;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationProperties;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class RestURI {

   public static final String[] EMPTY_STRING_ARRAY = new String[0];

   public static RestURI create(String uriString) {
      return create(URI.create(uriString));
   }

   public static RestURI create(URI uri) {
      if (!"http".equals(uri.getScheme()) && !"https".equals(uri.getScheme())) {
         throw new IllegalArgumentException(uri.toString());
      }
      final List<InetSocketAddress> addresses;
      final String[] userInfo;
      if (uri.getHost() != null) {
         addresses = Collections.singletonList(InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort() < 0 ? RestClientConfigurationProperties.DEFAULT_REST_PORT : uri.getPort()));
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
            addresses.add(InetSocketAddress.createUnresolved(colon < 0 ? host : host.substring(0, colon), colon < 0 ? RestClientConfigurationProperties.DEFAULT_REST_PORT : Integer.parseInt(host.substring(colon + 1))));
         }
      }
      Properties properties = new Properties();
      if (uri.getQuery() != null) {
         String[] parts = uri.getQuery().split("&");
         for (String part : parts) {
            int eq = part.indexOf('=');
            if (eq < 0) {
               throw new IllegalArgumentException(part);
            } else {
               properties.setProperty(RestClientConfigurationProperties.ICR + part.substring(0, eq), part.substring(eq + 1));
            }
         }
      }
      return new RestURI(addresses, "https".equals(uri.getScheme()), userInfo.length > 0 ? userInfo[0] : null, userInfo.length > 1 ? userInfo[1] : null, properties);

   }

   private final List<InetSocketAddress> addresses;
   private final boolean ssl;
   private final String username;
   private final String password;
   private final Properties properties;


   private RestURI(List<InetSocketAddress> addresses, boolean ssl, String username, String password, Properties properties) {
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

   public RestClientConfigurationBuilder toConfigurationBuilder() {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      for(InetSocketAddress address : addresses) {
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
      return "HotRodURI{" +
            "addresses=" + addresses +
            ", ssl=" + ssl +
            ", username='" + username + '\'' +
            ", password='" + password + '\'' +
            ", properties=" + properties +
            '}';
   }
}
