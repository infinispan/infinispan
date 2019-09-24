package org.infinispan.cli.connection.rest;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;

import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.connection.Connector;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.configuration.SslConfigurationBuilder;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices
public class RestConnector implements Connector {
   private
   final Pattern HOST_PORT = Pattern.compile("(\\[[0-9A-Fa-f:]+\\]|[^:/?#]*)(?::(\\d*))");

   @Override
   public Connection getConnection(String connectionString, SSLContext sslContext) {
      try {
         RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
         if (connectionString == null || connectionString.isEmpty()) {
            builder.addServer().host("localhost").port(11222);
         } else {
            Matcher matcher = HOST_PORT.matcher(connectionString);
            if (matcher.matches()) {
               String host = matcher.group(1);
               String port = matcher.group(2);
               builder.addServer().host(host).port(port != null ? Integer.parseInt(port) : 11222);
            } else {
               URL url = new URL(connectionString);
               if (!url.getProtocol().equals("http") && !url.getProtocol().equals("https")) {
                  throw new IllegalArgumentException();
               }

               int port = url.getPort();
               builder.addServer().host(url.getHost()).port(port > 0 ? port : 11222);
               String userInfo = url.getUserInfo();
               if (userInfo != null) {
                  String[] split = userInfo.split(":");
                  builder.security().authentication().username(split[0]).password(split[1]);
               }
               if (url.getProtocol().equals("https")) {
                  SslConfigurationBuilder ssl = builder.security().ssl().enable();
                  if (sslContext != null) {
                     ssl.sslContext(sslContext);
                  }
               }
            }
         }
         return new RestConnection(builder);
      } catch (Exception e) {
         return null;
      }
   }
}
