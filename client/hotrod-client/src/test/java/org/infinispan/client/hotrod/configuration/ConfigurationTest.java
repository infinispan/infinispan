package org.infinispan.client.hotrod.configuration;

import static org.infinispan.client.hotrod.impl.ConfigurationProperties.ASYNC_EXECUTOR_FACTORY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.AUTH_CALLBACK_HANDLER;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.AUTH_CLIENT_SUBJECT;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.AUTH_SERVER_NAME;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.CONNECT_TIMEOUT;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.HASH_FUNCTION_PREFIX;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.KEY_SIZE_ESTIMATE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.KEY_STORE_CERTIFICATE_PASSWORD;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.KEY_STORE_FILE_NAME;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.KEY_STORE_PASSWORD;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.MAX_RETRIES;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.PROTOCOL_VERSION;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.REQUEST_BALANCING_STRATEGY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SASL_MECHANISM;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SASL_PROPERTIES_PREFIX;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.JAVA_SERIAL_WHITELIST;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SERVER_LIST;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SNI_HOST_NAME;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SO_TIMEOUT;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SSL_CONTEXT;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SSL_PROTOCOL;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_KEEP_ALIVE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_NO_DELAY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TRUST_STORE_FILE_NAME;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TRUST_STORE_PASSWORD;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.USE_AUTH;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.USE_SSL;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.VALUE_SIZE_ESTIMATE;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import javax.net.ssl.SSLContext;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.SomeAsyncExecutorFactory;
import org.infinispan.client.hotrod.SomeCustomConsistentHashV2;
import org.infinispan.client.hotrod.SomeRequestBalancingStrategy;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.security.BasicCallbackHandler;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.FileLookupFactory;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.configuration.ConfigurationTest", groups = "functional" )
public class ConfigurationTest {

   static final Map<String, Function<Configuration, ?>> OPTIONS = new HashMap<>();
   static final Map<Class<?>, Function<Object, Object>> TYPES = new HashMap<>();

   static {
      OPTIONS.put(ASYNC_EXECUTOR_FACTORY, c -> c.asyncExecutorFactory().factoryClass());
      OPTIONS.put(REQUEST_BALANCING_STRATEGY, c -> c.balancingStrategyFactory().get().getClass());
      OPTIONS.put("maxActive", c -> c.connectionPool().maxActive());
      OPTIONS.put("maxTotal", c -> c.connectionPool().maxTotal());
      OPTIONS.put("maxWait", c -> c.connectionPool().maxWait());
      OPTIONS.put("maxIdle", c -> c.connectionPool().maxIdle());
      OPTIONS.put("minIdle", c -> c.connectionPool().minIdle());
      OPTIONS.put("exhaustedAction", c -> c.connectionPool().exhaustedAction());
      OPTIONS.put("numTestsPerEvictionRun", c -> c.connectionPool().numTestsPerEvictionRun());
      OPTIONS.put("timeBetweenEvictionRunsMillis", c -> c.connectionPool().timeBetweenEvictionRuns());
      OPTIONS.put("minEvictableIdleTimeMillis", c -> c.connectionPool().minEvictableIdleTime());
      OPTIONS.put("testOnBorrow", c -> c.connectionPool().testOnBorrow());
      OPTIONS.put("testOnReturn", c -> c.connectionPool().testOnReturn());
      OPTIONS.put("testWhileIdle", c -> c.connectionPool().testWhileIdle());
      OPTIONS.put(CONNECT_TIMEOUT, Configuration::connectionTimeout);
      OPTIONS.put(PROTOCOL_VERSION, Configuration::version);
      OPTIONS.put(SO_TIMEOUT, Configuration::socketTimeout);
      OPTIONS.put(TCP_NO_DELAY, Configuration::tcpNoDelay);
      OPTIONS.put(TCP_KEEP_ALIVE, Configuration::tcpKeepAlive);
      OPTIONS.put(KEY_SIZE_ESTIMATE, Configuration::keySizeEstimate);
      OPTIONS.put(VALUE_SIZE_ESTIMATE, Configuration::valueSizeEstimate);
      OPTIONS.put(MAX_RETRIES, Configuration::maxRetries);
      OPTIONS.put(USE_SSL, c -> c.security().ssl().enabled());
      OPTIONS.put(KEY_STORE_FILE_NAME, c -> c.security().ssl().keyStoreFileName());
      OPTIONS.put(SNI_HOST_NAME, c -> c.security().ssl().sniHostName());
      OPTIONS.put(KEY_STORE_PASSWORD, c -> new String(c.security().ssl().keyStorePassword()));
      OPTIONS.put(KEY_STORE_CERTIFICATE_PASSWORD, c -> new String(c.security().ssl().keyStoreCertificatePassword()));
      OPTIONS.put(TRUST_STORE_FILE_NAME, c -> c.security().ssl().trustStoreFileName());
      OPTIONS.put(TRUST_STORE_PASSWORD, c -> new String(c.security().ssl().trustStorePassword()));
      OPTIONS.put(SSL_PROTOCOL, c -> c.security().ssl().protocol());
      OPTIONS.put(SSL_CONTEXT, c -> c.security().ssl().sslContext());
      OPTIONS.put(USE_AUTH, c -> c.security().authentication().enabled());
      OPTIONS.put(SASL_MECHANISM, c -> c.security().authentication().saslMechanism());
      OPTIONS.put(AUTH_CALLBACK_HANDLER, c -> c.security().authentication().callbackHandler());
      OPTIONS.put(AUTH_SERVER_NAME, c -> c.security().authentication().serverName());
      OPTIONS.put(AUTH_CLIENT_SUBJECT, c -> c.security().authentication().clientSubject());
      OPTIONS.put(SASL_PROPERTIES_PREFIX + ".A", c -> c.security().authentication().saslProperties().get("A"));
      OPTIONS.put(SASL_PROPERTIES_PREFIX + ".B", c -> c.security().authentication().saslProperties().get("B"));
      OPTIONS.put(SASL_PROPERTIES_PREFIX + ".C", c -> c.security().authentication().saslProperties().get("C"));
      OPTIONS.put(JAVA_SERIAL_WHITELIST, Configuration::serialWhitelist);

      TYPES.put(Boolean.class, b -> Boolean.toString((Boolean) b));
      TYPES.put(ExhaustedAction.class, e -> Integer.toString(((ExhaustedAction) e).ordinal()));
      TYPES.put(Class.class, c -> ((Class<?>) c).getName());
      TYPES.put(Integer.class, Object::toString);
      TYPES.put(Long.class, Object::toString);
      TYPES.put(String.class, s -> s);
      TYPES.put(SSLContext.class, s -> s);
      TYPES.put(MyCallbackHandler.class, c -> c);
      TYPES.put(Subject.class, s -> s);
      TYPES.put(ProtocolVersion.class, p -> p.toString());
      TYPES.put(mkClass(), l -> String.join(",", (List<String>) l));
   }

   private static Class<?> mkClass() {
      try {
         return Class.forName("java.util.Arrays$ArrayList");
      } catch (ClassNotFoundException e) {
         throw new RuntimeException(e);
      }
   }

   CallbackHandler callbackHandler = new MyCallbackHandler();

   public static class MyCallbackHandler implements CallbackHandler {
      @Override public void handle(Callback[] callbacks) {}
   }

   Subject clientSubject = new Subject();

   public void testConfiguration() {
      Map<String, String> saslProperties = new HashMap<>();
      saslProperties.put("A", "1");
      saslProperties.put("B", "2");
      saslProperties.put("C", "3");

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
         .addServer()
            .host("host1")
            .port(11222)
         .addServer()
            .host("host2")
            .port(11222)
         .asyncExecutorFactory()
            .factoryClass(SomeAsyncExecutorFactory.class)
         .balancingStrategy(SomeRequestBalancingStrategy.class)
         .connectionPool()
            .maxActive(100)
            .maxTotal(150)
            .maxWait(1000)
            .maxIdle(20)
            .minIdle(10)
            .exhaustedAction(ExhaustedAction.WAIT)
            .numTestsPerEvictionRun(5)
            .testOnBorrow(true)
            .testOnReturn(true)
            .testWhileIdle(false)
            .minEvictableIdleTime(12000)
            .timeBetweenEvictionRuns(15000)
         .connectionTimeout(100)
         .version(ProtocolVersion.PROTOCOL_VERSION_13)
         .consistentHashImpl(2, SomeCustomConsistentHashV2.class)
         .socketTimeout(100)
         .tcpNoDelay(false)
         .keySizeEstimate(128)
         .valueSizeEstimate(1024)
         .maxRetries(0)
         .tcpKeepAlive(true)
         .security()
            .ssl()
               .enable()
               .keyStoreFileName("my-key-store.file")
               .keyStorePassword("my-key-store.password".toCharArray())
               .keyStoreCertificatePassword("my-key-store-certificate.password".toCharArray())
               .trustStoreFileName("my-trust-store.file")
               .trustStorePassword("my-trust-store.password".toCharArray())
               .protocol("TLSv1.1")
         .security()
            .authentication()
               .enable()
               .saslMechanism("my-sasl-mechanism")
               .callbackHandler(callbackHandler)
               .serverName("my-server-name")
               .clientSubject(clientSubject)
               .saslProperties(saslProperties)
         .addJavaSerialWhiteList(".*Person.*", ".*Employee.*");

      Configuration configuration = builder.build();
      validateConfiguration(configuration);

      ConfigurationBuilder newBuilder = new ConfigurationBuilder();
      newBuilder.read(configuration);
      Configuration newConfiguration = newBuilder.build();
      validateConfiguration(newConfiguration);
   }

   public void testWithProperties() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      Properties p = new Properties();
      p.setProperty(SERVER_LIST, "host1:11222; host2:11222");
      p.setProperty(ASYNC_EXECUTOR_FACTORY, "org.infinispan.client.hotrod.SomeAsyncExecutorFactory");
      p.setProperty(REQUEST_BALANCING_STRATEGY, "org.infinispan.client.hotrod.SomeRequestBalancingStrategy");
      p.setProperty(HASH_FUNCTION_PREFIX + "." + 2, "org.infinispan.client.hotrod.SomeCustomConsistentHashV2");
      p.setProperty("maxActive", "100");
      p.setProperty("maxTotal", "150");
      p.setProperty("maxWait", "1000");
      p.setProperty("maxIdle", "20");
      p.setProperty("minIdle", "10");
      p.setProperty("exhaustedAction", "1");
      p.setProperty("numTestsPerEvictionRun", "5");
      p.setProperty("timeBetweenEvictionRunsMillis", "15000");
      p.setProperty("minEvictableIdleTimeMillis", "12000");
      p.setProperty("testOnBorrow", "true");
      p.setProperty("testOnReturn", "true");
      p.setProperty("testWhileIdle", "false");
      p.setProperty(CONNECT_TIMEOUT, "100");
      p.setProperty(PROTOCOL_VERSION, "1.3");
      p.setProperty(SO_TIMEOUT, "100");
      p.setProperty(TCP_NO_DELAY, "false");
      p.setProperty(TCP_KEEP_ALIVE, "true");
      p.setProperty(KEY_SIZE_ESTIMATE, "128");
      p.setProperty(VALUE_SIZE_ESTIMATE, "1024");
      p.setProperty(MAX_RETRIES, "0");
      p.setProperty(USE_SSL, "true");
      p.setProperty(KEY_STORE_FILE_NAME, "my-key-store.file");
      p.setProperty(KEY_STORE_PASSWORD, "my-key-store.password");
      p.setProperty(KEY_STORE_CERTIFICATE_PASSWORD, "my-key-store-certificate.password");
      p.setProperty(TRUST_STORE_FILE_NAME, "my-trust-store.file");
      p.setProperty(TRUST_STORE_PASSWORD, "my-trust-store.password");
      p.setProperty(SSL_PROTOCOL, "TLSv1.1");
      p.setProperty(USE_AUTH, "true");
      p.setProperty(SASL_MECHANISM, "my-sasl-mechanism");
      p.put(AUTH_CALLBACK_HANDLER, callbackHandler);
      p.setProperty(AUTH_SERVER_NAME, "my-server-name");
      p.put(AUTH_CLIENT_SUBJECT, clientSubject);
      p.setProperty(SASL_PROPERTIES_PREFIX + ".A", "1");
      p.setProperty(SASL_PROPERTIES_PREFIX + ".B", "2");
      p.setProperty(SASL_PROPERTIES_PREFIX + ".C", "3");
      p.setProperty(JAVA_SERIAL_WHITELIST, ".*Person.*,.*Employee.*");

      Configuration configuration = builder.withProperties(p).build();
      validateConfiguration(configuration);

      ConfigurationBuilder newBuilder = new ConfigurationBuilder();
      newBuilder.read(configuration);
      Configuration newConfiguration = newBuilder.build();
      validateConfiguration(newConfiguration);
   }

   public void testSSLContext() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.security()
            .ssl()
            .enable()
            .sslContext(getSSLContext());

      Configuration configuration = builder.build();
      validateSSLContextConfiguration(configuration);

      ConfigurationBuilder newBuilder = new ConfigurationBuilder();
      newBuilder.read(configuration);
      Configuration newConfiguration = newBuilder.build();
      validateSSLContextConfiguration(newConfiguration);
   }

   public void testSni() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.security()
              .ssl()
              .enable()
              .sslContext(getSSLContext())
              .sniHostName("sni");

      Configuration configuration = builder.build();
      validateSniContextConfiguration(configuration);

      ConfigurationBuilder newBuilder = new ConfigurationBuilder();
      newBuilder.read(configuration);
      Configuration newConfiguration = newBuilder.build();
      validateSniContextConfiguration(newConfiguration);
   }

   public void testWithPropertiesSSLContext() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      Properties p = new Properties();
      p.put(SSL_CONTEXT, getSSLContext());
      Configuration configuration = builder.withProperties(p).build();
      validateSSLContextConfiguration(configuration);

      ConfigurationBuilder newBuilder = new ConfigurationBuilder();
      newBuilder.read(configuration);
      Configuration newConfiguration = newBuilder.build();
      validateSSLContextConfiguration(newConfiguration);
   }

   public void testWithPropertiesSni() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      Properties p = new Properties();
      p.put(TRUST_STORE_FILE_NAME, "my-trust-store.file");
      p.put(TRUST_STORE_PASSWORD, "my-trust-store.password");
      p.put(SNI_HOST_NAME, "sni");
      Configuration configuration = builder.withProperties(p).build();
      validateSniContextConfiguration(configuration);

      ConfigurationBuilder newBuilder = new ConfigurationBuilder();
      newBuilder.read(configuration);
      Configuration newConfiguration = newBuilder.build();
      validateSniContextConfiguration(newConfiguration);
   }

   public void testWithPropertiesAuthCallbackHandlerFQN() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      Properties p = new Properties();
      p.setProperty(AUTH_CALLBACK_HANDLER, MyCallbackHandler.class.getName());
      Configuration configuration = builder.withProperties(p).build();
      assertTrue(OPTIONS.get(AUTH_CALLBACK_HANDLER).apply(configuration) instanceof MyCallbackHandler);
   }

   public void testParseServerAddresses() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServers("1.1.1.1:9999");
      builder.addServers("2.2.2.2");
      builder.addServers("[fe80::290:bff:fe1b:5762]:7777");
      builder.addServers("[ff01::1]");
      builder.addServers("localhost");
      builder.addServers("localhost:8382");
      Configuration cfg = builder.build();
      assertServer("1.1.1.1", 9999, cfg.servers().get(0));
      assertServer("2.2.2.2", ConfigurationProperties.DEFAULT_HOTROD_PORT, cfg.servers().get(1));
      assertServer("fe80::290:bff:fe1b:5762", 7777, cfg.servers().get(2));
      assertServer("ff01::1", ConfigurationProperties.DEFAULT_HOTROD_PORT, cfg.servers().get(3));
      assertServer("localhost", ConfigurationProperties.DEFAULT_HOTROD_PORT, cfg.servers().get(4));
      assertServer("localhost", 8382, cfg.servers().get(5));
   }

   public void testPropertyReplacement() throws IOException, UnsupportedCallbackException {
      System.setProperty("test.property.server_list","myhost:12345");
      System.setProperty("test.property.marshaller", "org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller");
      System.setProperty("test.property.tcp_no_delay", "false");
      System.setProperty("test.property.tcp_keep_alive", "true");
      System.setProperty("test.property.key_size_estimate", "128");
      System.setProperty("test.property.value_size_estimate", "256");
      System.setProperty("test.property.maxTotal", "79");
      System.setProperty("test.property.maxActive", "78");
      System.setProperty("test.property.maxIdle", "77");
      System.setProperty("test.property.minIdle", "76");
      System.setProperty("test.property.timeBetweenEvictionRunsMillis", "1000");
      System.setProperty("test.property.minEvictableIdleTimeMillis", "2000");
      System.setProperty("test.property.testWhileIdle", "true");
      System.setProperty("test.property.use_auth", "true");
      System.setProperty("test.property.auth_username", "testuser");
      System.setProperty("test.property.auth_password", "testpassword");
      System.setProperty("test.property.auth_realm", "testrealm");
      System.setProperty("test.property.sasl_mechanism", "PLAIN");


      ConfigurationBuilder builder = new ConfigurationBuilder();
      Properties p = new Properties();
      InputStream inputStream = FileLookupFactory.newInstance().lookupFile("hotrod-client-replacement.properties", this.getClass().getClassLoader());
      p.load(inputStream);
      builder.withProperties(p);
      Configuration cfg = builder.build();
      assertServer("myhost", 12345, cfg.servers().get(0));
      assertEquals(ProtoStreamMarshaller.class, cfg.marshallerClass());
      assertFalse(cfg.tcpNoDelay());
      assertTrue(cfg.tcpKeepAlive());
      assertEquals(128, cfg.keySizeEstimate());
      assertEquals(256, cfg.valueSizeEstimate());
      assertEquals(79, cfg.connectionPool().maxTotal());
      assertEquals(78, cfg.connectionPool().maxActive());
      assertEquals(77, cfg.connectionPool().maxIdle());
      assertEquals(76, cfg.connectionPool().minIdle());
      assertEquals(1000, cfg.connectionPool().timeBetweenEvictionRuns());
      assertEquals(2000, cfg.connectionPool().minEvictableIdleTime());
      assertTrue(cfg.connectionPool().testWhileIdle());
      assertTrue(cfg.security().authentication().enabled());
      assertEquals("PLAIN", cfg.security().authentication().saslMechanism());
      CallbackHandler callbackHandler = cfg.security().authentication().callbackHandler();
      assertEquals(BasicCallbackHandler.class, callbackHandler.getClass());
      NameCallback nameCallback = new NameCallback("name");
      callbackHandler.handle(new Callback[] { nameCallback });
      assertEquals("testuser", nameCallback.getName());
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "ISPN(\\d)*: Invalid max_retries \\(value=-1\\). " +
               "Value should be greater or equal than zero.")
   public void testNegativeRetriesPerServer() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.maxRetries(-1);
      builder.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testMissingClusterNameDefinition() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addCluster(null);
      builder.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testMissingHostDefinition() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addCluster("test").addClusterNode(null, 1234);
      builder.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testMissingClusterServersDefinition() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addCluster("test");
      builder.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testDuplicateClusterDefinition() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addCluster("test").addClusterNode("host1", 1234);
      builder.addCluster("test").addClusterNode("host1", 5678);
      builder.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testInvalidAuthenticationConfig() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.security().authentication().enable().saslMechanism("PLAIN");
      builder.build();
   }

   public void testValidAuthenticationSubjectNoCBH() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.security().authentication().enable().saslMechanism("PLAIN").clientSubject(new Subject());
      builder.build();
   }

   public void testValidAuthenticationCBHNoSubject() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.security().authentication().enable().saslMechanism("PLAIN").callbackHandler(callbacks -> {});
      builder.build();
   }

   public void testClusters() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServers("1.1.1.1:9999");
      builder.addCluster("my-cluster").addClusterNode("localhost", 8382);
      Configuration cfg = builder.build();
      assertEquals(1, cfg.servers().size());
      assertServer("1.1.1.1", 9999, cfg.servers().get(0));
      assertEquals(1, cfg.clusters().size());
      assertEquals(1, cfg.clusters().get(0).getCluster().size());
      assertServer("localhost", 8382, cfg.clusters().get(0).getCluster().get(0));
   }

   private void assertServer(String host, int port, ServerConfiguration serverCfg) {
      assertEquals(host, serverCfg.host());
      assertEquals(port, serverCfg.port());
   }

   private void validateConfiguration(Configuration configuration) {
      assertEquals(2, configuration.servers().size());
      for (int i = 0; i < configuration.servers().size(); i++) {
         assertEquals(String.format("host%d", i+1), configuration.servers().get(i).host());
         assertEquals(11222, configuration.servers().get(i).port());
      }
      assertEqualsConfig(SomeAsyncExecutorFactory.class, ASYNC_EXECUTOR_FACTORY, configuration);
      assertEqualsConfig(SomeRequestBalancingStrategy.class, REQUEST_BALANCING_STRATEGY, configuration);
      assertEquals(null, configuration.consistentHashImpl(1));
      assertEquals(SomeCustomConsistentHashV2.class, configuration.consistentHashImpl(2));
      assertEqualsConfig(100, "maxActive", configuration);
      assertEqualsConfig(150, "maxTotal", configuration);
      assertEqualsConfig(1000L, "maxWait", configuration);
      assertEqualsConfig(20, "maxIdle", configuration);
      assertEqualsConfig(10, "minIdle", configuration);
      assertEqualsConfig(ExhaustedAction.WAIT, "exhaustedAction", configuration);
      assertEqualsConfig(5, "numTestsPerEvictionRun", configuration);
      assertEqualsConfig(15000L, "timeBetweenEvictionRunsMillis", configuration);
      assertEqualsConfig(12000L, "minEvictableIdleTimeMillis", configuration);
      assertEqualsConfig(true, "testOnBorrow", configuration);
      assertEqualsConfig(true, "testOnReturn", configuration);
      assertEqualsConfig(false, "testWhileIdle", configuration);
      assertEqualsConfig(100, CONNECT_TIMEOUT, configuration);
      assertEqualsConfig(100, SO_TIMEOUT, configuration);
      assertEqualsConfig(false, TCP_NO_DELAY, configuration);
      assertEqualsConfig(true, TCP_KEEP_ALIVE, configuration);
      assertEqualsConfig(128, KEY_SIZE_ESTIMATE, configuration);
      assertEqualsConfig(1024, VALUE_SIZE_ESTIMATE, configuration);
      assertEqualsConfig(0, MAX_RETRIES, configuration);
      assertEqualsConfig(true, USE_SSL, configuration);
      assertEqualsConfig("my-key-store.file", KEY_STORE_FILE_NAME, configuration);
      assertEqualsConfig("my-key-store.password", KEY_STORE_PASSWORD, configuration);
      assertEqualsConfig("my-key-store-certificate.password", KEY_STORE_CERTIFICATE_PASSWORD, configuration);
      assertEqualsConfig("my-trust-store.file", TRUST_STORE_FILE_NAME, configuration);
      assertEqualsConfig("my-trust-store.password", TRUST_STORE_PASSWORD, configuration);
      assertEqualsConfig("TLSv1.1", SSL_PROTOCOL, configuration);
      assertEqualsConfig(true, USE_AUTH, configuration);
      assertEqualsConfig("my-sasl-mechanism", SASL_MECHANISM, configuration);
      assertEqualsConfig(callbackHandler, AUTH_CALLBACK_HANDLER, configuration);
      assertEqualsConfig("my-server-name", AUTH_SERVER_NAME, configuration);
      assertEqualsConfig(clientSubject, AUTH_CLIENT_SUBJECT, configuration);
      assertEqualsConfig("1", SASL_PROPERTIES_PREFIX + ".A", configuration);
      assertEqualsConfig("2", SASL_PROPERTIES_PREFIX + ".B", configuration);
      assertEqualsConfig("3", SASL_PROPERTIES_PREFIX + ".C", configuration);
      assertEqualsConfig(ProtocolVersion.PROTOCOL_VERSION_13, PROTOCOL_VERSION, configuration);
      assertEqualsConfig(Arrays.asList(".*Person.*", ".*Employee.*"), JAVA_SERIAL_WHITELIST, configuration);
   }

   private void validateSSLContextConfiguration(Configuration configuration) {
      assertEqualsConfig(getSSLContext(), SSL_CONTEXT, configuration);
   }

   private void validateSniContextConfiguration(Configuration configuration) {
      assertEqualsConfig("sni", SNI_HOST_NAME, configuration);
   }

   private SSLContext getSSLContext() {
      try {
         return SSLContext.getDefault();
      } catch (NoSuchAlgorithmException e) {
         throw new RuntimeException(e);
      }
   }

   private static void assertEqualsConfig(Object expected, String propertyName, Configuration cfg) {
      assertEquals(expected, OPTIONS.get(propertyName).apply(cfg));
      assertEquals(TYPES.get(expected.getClass()).apply(expected), cfg.properties().get(propertyName));
   }

}
