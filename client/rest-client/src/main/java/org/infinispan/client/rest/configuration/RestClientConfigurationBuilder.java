package org.infinispan.client.rest.configuration;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.client.rest.RestURI;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.util.TypedProperties;

/**
 * <p>ConfigurationBuilder used to generate immutable {@link RestClientConfiguration} objects.
 *
 * <p>If you prefer to configure the client declaratively, see {@link org.infinispan.client.rest.configuration}</p>
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public class RestClientConfigurationBuilder implements RestClientConfigurationChildBuilder, Builder<RestClientConfiguration> {

   // Match IPv4 (host:port) or IPv6 ([host]:port) addresses
   private static final Pattern ADDRESS_PATTERN = Pattern
         .compile("(\\[([0-9A-Fa-f:]+)\\]|([^:/?#]*))(?::(\\d*))?");

   private long connectionTimeout = RestClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT;
   private long socketTimeout = RestClientConfigurationProperties.DEFAULT_SO_TIMEOUT;

   private final List<ServerConfigurationBuilder> servers;
   private final SecurityConfigurationBuilder security;
   private boolean tcpNoDelay = true;
   private boolean tcpKeepAlive = false;
   private Protocol protocol = Protocol.HTTP_11;
   private String contextPath = "/rest";
   private boolean priorKnowledge;
   private boolean followRedirects = true;

   public RestClientConfigurationBuilder() {
      this.security = new SecurityConfigurationBuilder(this);
      this.servers = new ArrayList<>();
   }

   @Override
   public ServerConfigurationBuilder addServer() {
      ServerConfigurationBuilder builder = new ServerConfigurationBuilder(this);
      this.servers.add(builder);
      return builder;
   }

   @Override
   public RestClientConfigurationBuilder addServers(String servers) {
      parseServers(servers, (host, port) -> addServer().host(host).port(port));
      return this;
   }

   public RestClientConfigurationBuilder clearServers() {
      this.servers.clear();
      return this;
   }

   private static void parseServers(String servers, BiConsumer<String, Integer> c) {
      for (String server : servers.split(";")) {
         Matcher matcher = ADDRESS_PATTERN.matcher(server.trim());
         if (matcher.matches()) {
            String v6host = matcher.group(2);
            String v4host = matcher.group(3);
            String host = v6host != null ? v6host : v4host;
            String portString = matcher.group(4);
            int port = portString == null
                  ? RestClientConfigurationProperties.DEFAULT_REST_PORT
                  : Integer.parseInt(portString);
            c.accept(host, port);
         } else {
            throw new IllegalArgumentException(server);
         }

      }
   }

   @Override
   public RestClientConfigurationBuilder protocol(Protocol protocol) {
      this.protocol = protocol;
      return this;
   }

   @Override
   public RestClientConfigurationBuilder priorKnowledge(boolean enabled) {
      this.priorKnowledge = enabled;
      return this;
   }

   @Override
   public RestClientConfigurationBuilder followRedirects(boolean followRedirects) {
      this.followRedirects = followRedirects;
      return this;
   }

   @Override
   public RestClientConfigurationBuilder connectionTimeout(long connectionTimeout) {
      this.connectionTimeout = connectionTimeout;
      return this;
   }

   @Override
   public SecurityConfigurationBuilder security() {
      return security;
   }

   @Override
   public RestClientConfigurationBuilder socketTimeout(long socketTimeout) {
      this.socketTimeout = socketTimeout;
      return this;
   }

   @Override
   public RestClientConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      this.tcpNoDelay = tcpNoDelay;
      return this;
   }

   @Override
   public RestClientConfigurationBuilder tcpKeepAlive(boolean keepAlive) {
      this.tcpKeepAlive = keepAlive;
      return this;
   }

   public RestClientConfigurationBuilder contextPath(String contextPath) {
      this.contextPath = contextPath;
      return this;
   }

   public RestClientConfigurationBuilder uri(URI uri) {
      this.read(RestURI.create(uri).toConfigurationBuilder().build(false));
      return this;
   }

   public RestClientConfigurationBuilder uri(String uri) {
      this.read(RestURI.create(uri).toConfigurationBuilder().build(false));
      return this;
   }

   @Override
   public RestClientConfigurationBuilder withProperties(Properties properties) {
      TypedProperties typed = TypedProperties.toTypedProperties(properties);
      this.protocol(typed.getEnumProperty(RestClientConfigurationProperties.PROTOCOL, Protocol.class, protocol, true));
      this.connectionTimeout(typed.getLongProperty(RestClientConfigurationProperties.CONNECT_TIMEOUT, connectionTimeout, true));
      String serverList = typed.getProperty(RestClientConfigurationProperties.SERVER_LIST, null, true);
      if (serverList != null) {
         this.servers.clear();
         this.addServers(serverList);
      }
      this.contextPath(typed.getProperty(RestClientConfigurationProperties.CONTEXT_PATH, contextPath, true));
      this.socketTimeout(typed.getLongProperty(RestClientConfigurationProperties.SO_TIMEOUT, socketTimeout, true));
      this.tcpNoDelay(typed.getBooleanProperty(RestClientConfigurationProperties.TCP_NO_DELAY, tcpNoDelay, true));
      this.tcpKeepAlive(typed.getBooleanProperty(RestClientConfigurationProperties.TCP_KEEP_ALIVE, tcpKeepAlive, true));
      this.security.ssl().withProperties(properties);
      this.security.authentication().withProperties(properties);

      return this;
   }

   @Override
   public void validate() {
      security.validate();
   }

   @Override
   public RestClientConfiguration create() {
      List<ServerConfiguration> servers = new ArrayList<>();
      if (this.servers.size() > 0)
         for (ServerConfigurationBuilder server : this.servers) {
            servers.add(server.create());
         }
      else {
         servers.add(new ServerConfiguration("127.0.0.1", RestClientConfigurationProperties.DEFAULT_REST_PORT));
      }

      return new RestClientConfiguration(servers, protocol, connectionTimeout, socketTimeout, security.create(), tcpNoDelay, tcpKeepAlive, contextPath, priorKnowledge, followRedirects);
   }


   @Override
   public RestClientConfiguration build() {
      return build(true);
   }

   public RestClientConfiguration build(boolean validate) {
      if (validate) {
         validate();
      }
      return create();
   }

   @Override
   public RestClientConfigurationBuilder read(RestClientConfiguration template) {
      this.connectionTimeout = template.connectionTimeout();
      this.servers.clear();
      for (ServerConfiguration server : template.servers()) {
         this.addServer().host(server.host()).port(server.port());
      }
      this.socketTimeout = template.socketTimeout();
      this.security.read(template.security());
      this.tcpNoDelay = template.tcpNoDelay();
      this.tcpKeepAlive = template.tcpKeepAlive();
      this.security.read(template.security());

      return this;
   }
}
