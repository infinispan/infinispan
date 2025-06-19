package org.infinispan.client.openapi.configuration;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.client.openapi.OpenAPIURI;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Version;

/**
 * <p>ConfigurationBuilder used to generate immutable {@link OpenAPIClientConfiguration} objects.
 *
 * <p>If you prefer to configure the client declaratively, see {@link org.infinispan.client.rest.configuration}</p>
 *
 * @author Tristan Tarrant
 * @since 16.0
 */
public class OpenAPIClientConfigurationBuilder implements OpenAPIClientConfigurationChildBuilder, Builder<OpenAPIClientConfiguration> {

   // Match IPv4 (host:port) or IPv6 ([host]:port) addresses
   private static final Pattern ADDRESS_PATTERN = Pattern
         .compile("(\\[([0-9A-Fa-f:]+)]|([^:/?#]*))(?::(\\d*))?");

   private long connectionTimeout = OpenAPIClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT;
   private long socketTimeout = OpenAPIClientConfigurationProperties.DEFAULT_SO_TIMEOUT;

   private final List<ServerConfigurationBuilder> servers;
   private final SecurityConfigurationBuilder security;
   private boolean tcpNoDelay = true;
   private boolean tcpKeepAlive = false;
   private Protocol protocol = Protocol.HTTP_20;
   private String contextPath = OpenAPIClientConfigurationProperties.DEFAULT_CONTEXT_PATH;
   private boolean priorKnowledge;
   private boolean followRedirects = true;
   private boolean pingOnCreate = true;
   private final Map<String, String> headers = new HashMap<>();
   private ExecutorService executorService;

   public OpenAPIClientConfigurationBuilder() {
      this.security = new SecurityConfigurationBuilder(this);
      this.servers = new ArrayList<>();
      this.headers.put("User-Agent", Version.getBrandName() + "/" + Version.getVersion());
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }

   @Override
   public ServerConfigurationBuilder addServer() {
      ServerConfigurationBuilder builder = new ServerConfigurationBuilder(this);
      this.servers.add(builder);
      return builder;
   }

   @Override
   public OpenAPIClientConfigurationBuilder addServers(String servers) {
      parseServers(servers, (host, port) -> addServer().host(host).port(port));
      return this;
   }

   public OpenAPIClientConfigurationBuilder clearServers() {
      this.servers.clear();
      return this;
   }

   public List<ServerConfigurationBuilder> servers() {
      return servers;
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
                  ? OpenAPIClientConfigurationProperties.DEFAULT_REST_PORT
                  : Integer.parseInt(portString);
            c.accept(host, port);
         } else {
            throw new IllegalArgumentException(server);
         }

      }
   }

   @Override
   public OpenAPIClientConfigurationBuilder protocol(Protocol protocol) {
      this.protocol = protocol;
      return this;
   }

   @Override
   public OpenAPIClientConfigurationBuilder priorKnowledge(boolean enabled) {
      this.priorKnowledge = enabled;
      return this;
   }

   @Override
   public OpenAPIClientConfigurationBuilder followRedirects(boolean followRedirects) {
      this.followRedirects = followRedirects;
      return this;
   }

   @Override
   public OpenAPIClientConfigurationBuilder connectionTimeout(long connectionTimeout) {
      this.connectionTimeout = connectionTimeout;
      return this;
   }

   @Override
   public OpenAPIClientConfigurationBuilder pingOnCreate(boolean pingOnCreate) {
      this.pingOnCreate = pingOnCreate;
      return this;
   }

   @Override
   public SecurityConfigurationBuilder security() {
      return security;
   }

   @Override
   public OpenAPIClientConfigurationBuilder socketTimeout(long socketTimeout) {
      this.socketTimeout = socketTimeout;
      return this;
   }

   @Override
   public OpenAPIClientConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      this.tcpNoDelay = tcpNoDelay;
      return this;
   }

   @Override
   public OpenAPIClientConfigurationBuilder tcpKeepAlive(boolean keepAlive) {
      this.tcpKeepAlive = keepAlive;
      return this;
   }

   public OpenAPIClientConfigurationBuilder contextPath(String contextPath) {
      this.contextPath = contextPath;
      return this;
   }

   public OpenAPIClientConfigurationBuilder uri(URI uri) {
      this.read(OpenAPIURI.create(uri).toConfigurationBuilder().build(false));
      return this;
   }

   public OpenAPIClientConfigurationBuilder uri(String uri) {
      this.read(OpenAPIURI.create(uri).toConfigurationBuilder().build(false));
      return this;
   }

   public OpenAPIClientConfigurationBuilder header(String name, String value) {
      headers.put(name, value);
      return this;
   }

   public OpenAPIClientConfigurationBuilder executorService(ExecutorService executorService) {
      this.executorService = executorService;
      return this;
   }

   @Override
   public OpenAPIClientConfigurationBuilder withProperties(Properties properties) {
      TypedProperties typed = TypedProperties.toTypedProperties(properties);
      this.protocol(typed.getEnumProperty(OpenAPIClientConfigurationProperties.PROTOCOL, Protocol.class, protocol, true));
      this.connectionTimeout(typed.getLongProperty(OpenAPIClientConfigurationProperties.CONNECT_TIMEOUT, connectionTimeout, true));
      String serverList = typed.getProperty(OpenAPIClientConfigurationProperties.SERVER_LIST, null, true);
      if (serverList != null) {
         this.servers.clear();
         this.addServers(serverList);
      }
      this.contextPath(typed.getProperty(OpenAPIClientConfigurationProperties.CONTEXT_PATH, contextPath, true));
      this.socketTimeout(typed.getLongProperty(OpenAPIClientConfigurationProperties.SO_TIMEOUT, socketTimeout, true));
      this.tcpNoDelay(typed.getBooleanProperty(OpenAPIClientConfigurationProperties.TCP_NO_DELAY, tcpNoDelay, true));
      this.tcpKeepAlive(typed.getBooleanProperty(OpenAPIClientConfigurationProperties.TCP_KEEP_ALIVE, tcpKeepAlive, true));
      this.security.ssl().withProperties(properties);
      this.security.authentication().withProperties(properties);

      return this;
   }

   @Override
   public void validate() {
      security.validate();
   }

   @Override
   public OpenAPIClientConfiguration create() {
      List<ServerConfiguration> servers = new ArrayList<>();
      if (!this.servers.isEmpty())
         for (ServerConfigurationBuilder server : this.servers) {
            servers.add(server.create());
         }
      else {
         servers.add(new ServerConfiguration("127.0.0.1", OpenAPIClientConfigurationProperties.DEFAULT_REST_PORT));
      }

      return new OpenAPIClientConfiguration(servers, protocol, connectionTimeout, socketTimeout, security.create(),
            tcpNoDelay, tcpKeepAlive, contextPath, priorKnowledge, followRedirects, pingOnCreate, headers, executorService);
   }


   @Override
   public OpenAPIClientConfiguration build() {
      return build(true);
   }

   public OpenAPIClientConfiguration build(boolean validate) {
      if (validate) {
         validate();
      }
      return create();
   }

   @Override
   public OpenAPIClientConfigurationBuilder read(OpenAPIClientConfiguration template, Combine combine) {
      this.connectionTimeout = template.connectionTimeout();
      if (combine.repeatedAttributes() == Combine.RepeatedAttributes.OVERRIDE) {
         this.servers.clear();
         this.headers.clear();
      }
      for (ServerConfiguration server : template.servers()) {
         this.addServer().host(server.host()).port(server.port());
      }
      this.socketTimeout = template.socketTimeout();
      this.security.read(template.security(), combine);
      this.tcpNoDelay = template.tcpNoDelay();
      this.tcpKeepAlive = template.tcpKeepAlive();
      this.security.read(template.security(), combine);
      this.headers.putAll(template.headers());
      this.contextPath = template.contextPath();
      return this;
   }
}
