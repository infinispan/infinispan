package org.infinispan.client.hotrod.configuration;

import static org.infinispan.client.hotrod.impl.ConfigurationProperties.ASYNC_EXECUTOR_FACTORY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.AUTH_CALLBACK_HANDLER;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.AUTH_CLIENT_SUBJECT;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.AUTH_PASSWORD;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.AUTH_REALM;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.AUTH_SERVER_NAME;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.AUTH_USERNAME;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.CLUSTER_PROPERTIES_PREFIX;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.CONNECTION_POOL_EXHAUSTED_ACTION;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.CONNECTION_POOL_MAX_ACTIVE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.CONNECTION_POOL_MAX_PENDING_REQUESTS;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.CONNECTION_POOL_MAX_WAIT;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.CONNECTION_POOL_MIN_IDLE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.CONNECT_TIMEOUT;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.HASH_FUNCTION_PREFIX;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.JAVA_SERIAL_ALLOWLIST;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.JMX;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.JMX_DOMAIN;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.JMX_NAME;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.KEY_SIZE_ESTIMATE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.KEY_STORE_FILE_NAME;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.KEY_STORE_PASSWORD;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.MAX_RETRIES;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.PROTOCOL_VERSION;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.REQUEST_BALANCING_STRATEGY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SASL_MECHANISM;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SASL_PROPERTIES_PREFIX;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SERVER_LIST;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SNI_HOST_NAME;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SO_TIMEOUT;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SSL_CONTEXT;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SSL_HOSTNAME_VALIDATION;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SSL_PROTOCOL;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.STATISTICS;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_KEEP_ALIVE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_NO_DELAY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TRACING_PROPAGATION_ENABLED;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TRUST_STORE_FILE_NAME;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TRUST_STORE_PASSWORD;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.USE_AUTH;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.USE_SSL;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.VALUE_SIZE_ESTIMATE;
import static org.infinispan.commons.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

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
import org.infinispan.client.hotrod.impl.HotRodURI;
import org.infinispan.client.hotrod.security.BasicCallbackHandler;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.transaction.lookup.RemoteTransactionManagerLookup;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.configuration.ConfigurationTest", groups = "functional")
public class ConfigurationTest extends AbstractInfinispanTest {

   static final Map<String, Function<Configuration, ?>> OPTIONS = new HashMap<>();
   static final Map<Class<?>, Function<Object, Object>> TYPES = new HashMap<>();

   static {
      OPTIONS.put(ASYNC_EXECUTOR_FACTORY, c -> c.asyncExecutorFactory().factoryClass());
      OPTIONS.put(REQUEST_BALANCING_STRATEGY, c -> c.balancingStrategyFactory().get().getClass());
      OPTIONS.put("maxActive", c -> c.connectionPool().maxActive());
      OPTIONS.put(CONNECTION_POOL_MAX_ACTIVE, c -> c.connectionPool().maxActive());
      OPTIONS.put("maxWait", c -> c.connectionPool().maxWait());
      OPTIONS.put(CONNECTION_POOL_MAX_WAIT, c -> c.connectionPool().maxWait());
      OPTIONS.put("minIdle", c -> c.connectionPool().minIdle());
      OPTIONS.put(CONNECTION_POOL_MIN_IDLE, c -> c.connectionPool().minIdle());
      OPTIONS.put("exhaustedAction", c -> c.connectionPool().exhaustedAction());
      OPTIONS.put(CONNECTION_POOL_EXHAUSTED_ACTION, c -> c.connectionPool().exhaustedAction());
      OPTIONS.put("minEvictableIdleTimeMillis", c -> c.connectionPool().minEvictableIdleTime());
      OPTIONS.put(CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME, c -> c.connectionPool().minEvictableIdleTime());
      OPTIONS.put(CONNECTION_POOL_MAX_PENDING_REQUESTS, c -> c.connectionPool().maxPendingRequests());
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
      OPTIONS.put(JAVA_SERIAL_ALLOWLIST, Configuration::serialAllowList);

      TYPES.put(Boolean.class, b -> Boolean.toString((Boolean) b));
      TYPES.put(ExhaustedAction.class, Object::toString);
      TYPES.put(Class.class, c -> ((Class<?>) c).getName());
      TYPES.put(Integer.class, Object::toString);
      TYPES.put(Long.class, Object::toString);
      TYPES.put(String.class, Function.identity());
      TYPES.put(SSLContext.class, Function.identity());
      TYPES.put(MyCallbackHandler.class, Function.identity());
      TYPES.put(Subject.class, Function.identity());
      TYPES.put(ProtocolVersion.class, Object::toString);
      TYPES.put(NearCacheMode.class, Object::toString);
      TYPES.put(mkClass(), l -> String.join(",", (List<String>) l));
      TYPES.put(Pattern.class, Function.identity());
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
      @Override
      public void handle(Callback[] callbacks) {
      }
   }

   Subject clientSubject = new Subject();

   public void testConfiguration() {
      Map<String, String> saslProperties = new HashMap<>();
      saslProperties.put("A", "1");
      saslProperties.put("B", "2");
      saslProperties.put("C", "3");

      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder
            .statistics().enable().jmxEnable().jmxDomain("jmxInfinispanDomain").jmxName("jmxInfinispan")
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
            .maxWait(1000)
            .minIdle(10)
            .minEvictableIdleTime(12000)
            .exhaustedAction(ExhaustedAction.WAIT)
            .maxPendingRequests(12)
            .connectionTimeout(100)
            .version(ProtocolVersion.PROTOCOL_VERSION_30)
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
            .sniHostName("infinispan.test")
            .keyStoreFileName("my-key-store.file")
            .keyStorePassword("my-key-store.password".toCharArray())
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
            .addJavaSerialAllowList(".*Person.*", ".*Employee.*")
            .addCluster("siteA")
            .addClusterNode("hostA1", 11222)
            .addClusterNode("hostA2", 11223)
            .addCluster("siteB")
            .addClusterNodes("hostB1:11222; hostB2:11223");

      Configuration configuration = builder.build();
      validateConfiguration(configuration);

      ConfigurationBuilder newBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      newBuilder.read(configuration);
      Configuration newConfiguration = newBuilder.build();
      validateConfiguration(newConfiguration);
   }

   public void testWithProperties() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      Properties p = new Properties();
      p.setProperty(SERVER_LIST, "host1:11222; host2:11222");
      p.setProperty(ASYNC_EXECUTOR_FACTORY, "org.infinispan.client.hotrod.SomeAsyncExecutorFactory");
      p.setProperty(REQUEST_BALANCING_STRATEGY, "org.infinispan.client.hotrod.SomeRequestBalancingStrategy");
      p.setProperty(HASH_FUNCTION_PREFIX + "." + 2, "org.infinispan.client.hotrod.SomeCustomConsistentHashV2");
      p.setProperty(CONNECTION_POOL_MAX_ACTIVE, "100");
      p.setProperty("maxTotal", "150");
      p.setProperty(CONNECTION_POOL_MAX_WAIT, "1000");
      p.setProperty("maxIdle", "20");
      p.setProperty(CONNECTION_POOL_MIN_IDLE, "10");
      p.setProperty(CONNECTION_POOL_EXHAUSTED_ACTION, ExhaustedAction.WAIT.name());
      p.setProperty("numTestsPerEvictionRun", "5");
      p.setProperty("timeBetweenEvictionRunsMillis", "15000");
      p.setProperty(CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME, "12000");
      p.setProperty(CONNECTION_POOL_MAX_PENDING_REQUESTS, "12");
      p.setProperty("testOnBorrow", "true");
      p.setProperty("testOnReturn", "true");
      p.setProperty("testWhileIdle", "false");
      p.setProperty(CONNECT_TIMEOUT, "100");
      p.setProperty(PROTOCOL_VERSION, "3.0");
      p.setProperty(SO_TIMEOUT, "100");
      p.setProperty(TCP_NO_DELAY, "false");
      p.setProperty(TCP_KEEP_ALIVE, "true");
      p.setProperty(KEY_SIZE_ESTIMATE, "128");
      p.setProperty(VALUE_SIZE_ESTIMATE, "1024");
      p.setProperty(MAX_RETRIES, "0");
      p.setProperty(USE_SSL, "true");
      p.setProperty(KEY_STORE_FILE_NAME, "my-key-store.file");
      p.setProperty(KEY_STORE_PASSWORD, "my-key-store.password");
      p.setProperty(TRUST_STORE_FILE_NAME, "my-trust-store.file");
      p.setProperty(TRUST_STORE_PASSWORD, "my-trust-store.password");
      p.setProperty(SSL_PROTOCOL, "TLSv1.1");
      p.setProperty(SNI_HOST_NAME, "infinispan.test");
      p.setProperty(USE_AUTH, "true");
      p.setProperty(SASL_MECHANISM, "my-sasl-mechanism");
      p.put(AUTH_CALLBACK_HANDLER, callbackHandler);
      p.setProperty(AUTH_SERVER_NAME, "my-server-name");
      p.put(AUTH_CLIENT_SUBJECT, clientSubject);
      p.setProperty(SASL_PROPERTIES_PREFIX + ".A", "1");
      p.setProperty(SASL_PROPERTIES_PREFIX + ".B", "2");
      p.setProperty(SASL_PROPERTIES_PREFIX + ".C", "3");
      p.setProperty(JAVA_SERIAL_ALLOWLIST, ".*Person.*,.*Employee.*");
      p.setProperty(CLUSTER_PROPERTIES_PREFIX + ".siteA", "hostA1:11222; hostA2:11223");
      p.setProperty(CLUSTER_PROPERTIES_PREFIX + ".siteB", "hostB1:11222; hostB2:11223");
      p.setProperty(STATISTICS, "true");
      p.setProperty(JMX, "true");
      p.setProperty(JMX_NAME, "jmxInfinispan");
      p.setProperty(JMX_DOMAIN, "jmxInfinispanDomain");
      p.setProperty(TRACING_PROPAGATION_ENABLED, "false");

      Configuration configuration = builder.withProperties(p).build();
      validateConfiguration(configuration);

      ConfigurationBuilder builderWithOtherTypes = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      p.replace(SO_TIMEOUT, 100); // adding an integer
      p.replace(CONNECTION_POOL_MAX_ACTIVE, Short.valueOf("100")); //adding a short
      configuration = builderWithOtherTypes.withProperties(p).build();
      validateConfiguration(configuration);

      ConfigurationBuilder newBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      newBuilder.read(configuration, Combine.DEFAULT);
      Configuration newConfiguration = newBuilder.build();
      validateConfiguration(newConfiguration);

      p.setProperty(PROTOCOL_VERSION, "auto");
      configuration = new ConfigurationBuilder().withProperties(p).build();
      assertEquals(ProtocolVersion.PROTOCOL_VERSION_AUTO, configuration.version());
      assertFalse(configuration.tracingPropagationEnabled());
   }

   public void testSSLContext() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.security()
            .ssl()
            .enable()
            .sniHostName("infinispan.test")
            .sslContext(getSSLContext());

      Configuration configuration = builder.build();
      validateSSLContextConfiguration(configuration);

      ConfigurationBuilder newBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      newBuilder.read(configuration, Combine.DEFAULT);
      Configuration newConfiguration = newBuilder.build();
      validateSSLContextConfiguration(newConfiguration);
   }

   public void testSni() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.security()
            .ssl()
            .enable()
            .sslContext(getSSLContext())
            .sniHostName("sni");

      Configuration configuration = builder.build();
      validateSniContextConfiguration(configuration);

      ConfigurationBuilder newBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      newBuilder.read(configuration, Combine.DEFAULT);
      Configuration newConfiguration = newBuilder.build();
      validateSniContextConfiguration(newConfiguration);
   }

   public void testWithPropertiesSSLContext() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      Properties p = new Properties();
      p.put(SSL_CONTEXT, getSSLContext());
      p.put(SSL_HOSTNAME_VALIDATION, false);
      Configuration configuration = builder.withProperties(p).build();
      validateSSLContextConfiguration(configuration);

      ConfigurationBuilder newBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      newBuilder.read(configuration, Combine.DEFAULT);
      Configuration newConfiguration = newBuilder.build();
      validateSSLContextConfiguration(newConfiguration);
   }

   public void testWithPropertiesSni() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      Properties p = new Properties();
      p.put(TRUST_STORE_FILE_NAME, "my-trust-store.file");
      p.put(TRUST_STORE_PASSWORD, "my-trust-store.password");
      p.put(SNI_HOST_NAME, "sni");
      Configuration configuration = builder.withProperties(p).build();
      validateSniContextConfiguration(configuration);

      ConfigurationBuilder newBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      newBuilder.read(configuration, Combine.DEFAULT);
      Configuration newConfiguration = newBuilder.build();
      validateSniContextConfiguration(newConfiguration);
   }

   public void testWithPropertiesAuthCallbackHandlerFQN() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      Properties p = new Properties();
      p.setProperty(AUTH_CALLBACK_HANDLER, MyCallbackHandler.class.getName());
      Configuration configuration = builder.withProperties(p).build();
      assertTrue(OPTIONS.get(AUTH_CALLBACK_HANDLER).apply(configuration) instanceof MyCallbackHandler);
   }

   public void testParseServerAddresses() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
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
      System.setProperty("test.property.server_list", "myhost:12345");
      System.setProperty("test.property.marshaller", "org.infinispan.commons.marshall.ProtoStreamMarshaller");
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


      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
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
      assertEquals(78, cfg.connectionPool().maxActive());
      assertEquals(76, cfg.connectionPool().minIdle());
      assertEquals(2000, cfg.connectionPool().minEvictableIdleTime());
      assertTrue(cfg.security().authentication().enabled());
      assertEquals("PLAIN", cfg.security().authentication().saslMechanism());
      CallbackHandler callbackHandler = cfg.security().authentication().callbackHandler();
      assertEquals(BasicCallbackHandler.class, callbackHandler.getClass());
      NameCallback nameCallback = new NameCallback("name");
      callbackHandler.handle(new Callback[]{nameCallback});
      assertEquals("testuser", nameCallback.getName());
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "ISPN(\\d)*: Invalid max_retries \\(value=-1\\). " +
               "Value should be greater or equal than zero.")
   public void testNegativeRetriesPerServer() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.maxRetries(-1);
      builder.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testMissingClusterNameDefinition() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addCluster(null);
      builder.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testMissingHostDefinition() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addCluster("test").addClusterNode(null, 1234);
      builder.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testMissingClusterServersDefinition() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addCluster("test");
      builder.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testDuplicateClusterDefinition() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addCluster("test").addClusterNode("host1", 1234);
      builder.addCluster("test").addClusterNode("host1", 5678);
      builder.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testInvalidAuthenticationConfig() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.security().authentication().enable().saslMechanism("PLAIN");
      builder.build();
   }

   public void testValidAuthenticationSubjectNoCBH() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.security().authentication().enable().saslMechanism("PLAIN").clientSubject(new Subject());
      builder.build();
   }

   public void testValidAuthenticationCBHNoSubject() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.security().authentication().enable().saslMechanism("PLAIN").callbackHandler(callbacks -> {
      });
      builder.build();
   }

   public void testClusters() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServers("1.1.1.1:9999");
      builder.addCluster("my-cluster").addClusterNode("localhost", 8382);
      Configuration cfg = builder.build();
      assertEquals(1, cfg.servers().size());
      assertServer("1.1.1.1", 9999, cfg.servers().get(0));
      assertEquals(1, cfg.clusters().size());
      assertEquals(1, cfg.clusters().get(0).getCluster().size());
      assertServer("localhost", 8382, cfg.clusters().get(0).getCluster().get(0));
   }

   public void testNoTransactionOverwrite() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.remoteCache("tx-cache")
            .transactionMode(TransactionMode.FULL_XA)
            .transactionManagerLookup(RemoteTransactionManagerLookup.getInstance());
      builder.transactionTimeout(1234, TimeUnit.MILLISECONDS);
      Properties p = new Properties();
      p.setProperty(SERVER_LIST, "host1:11222; host2:11222");
      p.setProperty(AUTH_USERNAME, "admin");
      p.setProperty(AUTH_PASSWORD, "password");
      p.setProperty(AUTH_REALM, "default");
      p.setProperty(SASL_MECHANISM, "SCRAM-SHA-512");
      builder.withProperties(p);
      Configuration config = builder.build();
      assertEquals(TransactionMode.FULL_XA, config.remoteCaches().get("tx-cache").transactionMode());
      assertEquals(RemoteTransactionManagerLookup.getInstance(), config.remoteCaches().get("tx-cache").transactionManagerLookup());
      assertEquals(1234, config.transactionTimeout());
      assertEquals(2, config.servers().size());
      assertServer("host1", 11222, config.servers().get(0));
      assertServer("host2", 11222, config.servers().get(1));

      assertEquals("SCRAM-SHA-512", config.security().authentication().saslMechanism());
      CallbackHandler ch = config.security().authentication().callbackHandler();
      assertEquals(BasicCallbackHandler.class, ch.getClass());
      BasicCallbackHandler bch = (BasicCallbackHandler) ch;
      assertEquals("admin", bch.getUsername());
      assertArrayEquals("password".toCharArray(), bch.getPassword());
      assertEquals("default", bch.getRealm());
   }

   public void testNoTransactionOverwriteWithProperties() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();

      Properties p = new Properties();
      p.setProperty("infinispan.client.hotrod.cache.tx-cache.transaction.transaction_mode", "FULL_XA");
      p.setProperty("infinispan.client.hotrod.cache.tx-cache.transaction.transaction_manager_lookup", RemoteTransactionManagerLookup.class.getName());
      p.setProperty("infinispan.client.hotrod.transaction.timeout", "1234");

      builder.withProperties(p);
      Configuration config = builder.build();
      assertEquals(TransactionMode.FULL_XA, config.remoteCaches().get("tx-cache").transactionMode());
      assertEquals(RemoteTransactionManagerLookup.getInstance(), config.remoteCaches().get("tx-cache").transactionManagerLookup());
      assertEquals(1234, config.transactionTimeout());
   }

   private void assertServer(String host, int port, ServerConfiguration serverCfg) {
      assertEquals(host, serverCfg.host());
      assertEquals(port, serverCfg.port());
   }

   private void validateConfiguration(Configuration configuration) {
      assertEquals(2, configuration.servers().size());
      for (int i = 0; i < configuration.servers().size(); i++) {
         assertEquals(String.format("host%d", i + 1), configuration.servers().get(i).host());
         assertEquals(11222, configuration.servers().get(i).port());
      }
      assertEqualsConfig(SomeAsyncExecutorFactory.class, ASYNC_EXECUTOR_FACTORY, configuration);
      assertEqualsConfig(SomeRequestBalancingStrategy.class, REQUEST_BALANCING_STRATEGY, configuration);
      assertNull(configuration.consistentHashImpl(1));
      assertEquals(SomeCustomConsistentHashV2.class, configuration.consistentHashImpl(2));
      assertEqualsConfig(100, "maxActive", configuration);
      assertEqualsConfig(100, CONNECTION_POOL_MAX_ACTIVE, configuration);
      assertEqualsConfig(1000L, "maxWait", configuration);
      assertEqualsConfig(1000L, CONNECTION_POOL_MAX_WAIT, configuration);
      assertEqualsConfig(10, "minIdle", configuration);
      assertEqualsConfig(10, CONNECTION_POOL_MIN_IDLE, configuration);
      assertEqualsConfig(ExhaustedAction.WAIT, CONNECTION_POOL_EXHAUSTED_ACTION, configuration);
      assertEqualsConfig(12000L, "minEvictableIdleTimeMillis", configuration);
      assertEqualsConfig(12000L, CONNECTION_POOL_MIN_EVICTABLE_IDLE_TIME, configuration);
      assertEqualsConfig(12, CONNECTION_POOL_MAX_PENDING_REQUESTS, configuration);
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
      assertEqualsConfig(ProtocolVersion.PROTOCOL_VERSION_30, PROTOCOL_VERSION, configuration);
      assertEqualsConfig(Arrays.asList(".*Person.*", ".*Employee.*"), JAVA_SERIAL_ALLOWLIST, configuration);
      assertEquals(2, configuration.clusters().size());
      assertEquals("siteA", configuration.clusters().get(0).getClusterName());
      assertEquals("hostA1", configuration.clusters().get(0).getCluster().get(0).host());
      assertEquals(11222, configuration.clusters().get(0).getCluster().get(0).port());
      assertEquals("hostA2", configuration.clusters().get(0).getCluster().get(1).host());
      assertEquals(11223, configuration.clusters().get(0).getCluster().get(1).port());
      assertEquals("siteB", configuration.clusters().get(1).getClusterName());
      assertEquals("hostB1", configuration.clusters().get(1).getCluster().get(0).host());
      assertEquals(11222, configuration.clusters().get(1).getCluster().get(0).port());
      assertEquals("hostB2", configuration.clusters().get(1).getCluster().get(1).host());
      assertEquals(11223, configuration.clusters().get(1).getCluster().get(1).port());
      assertTrue(configuration.statistics().enabled());
      assertTrue(configuration.statistics().jmxEnabled());
      assertEquals("jmxInfinispan", configuration.statistics().jmxName());
      assertEquals("jmxInfinispanDomain", configuration.statistics().jmxDomain());
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

   public void testMixOfConfiguration1() {
      ConfigurationBuilder cb = HotRodURI.create("hotrod://host1?client_intelligence=BASIC").toConfigurationBuilder();
      Properties properties = new Properties();
      properties.setProperty(ConfigurationProperties.DEFAULT_EXECUTOR_FACTORY_POOL_SIZE, "1");
      cb.asyncExecutorFactory().withExecutorProperties(properties);
      Configuration configuration = cb.build();
      assertEquals(ClientIntelligence.BASIC, configuration.clientIntelligence());
   }

   public void testMixOfConfiguration2() {
      ConfigurationBuilder configurationBuilder = HotRodURI.create("hotrod://host1?client_intelligence=BASIC").toConfigurationBuilder();
      configurationBuilder.maxRetries(1);
      Configuration configuration = configurationBuilder.build();
      assertEquals(ClientIntelligence.BASIC, configuration.clientIntelligence());
      assertEquals(1, configuration.maxRetries());

      configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.maxRetries(1);
      configurationBuilder.socketTimeout(123);
      configurationBuilder = configurationBuilder.uri("hotrod://host?client_intelligence=BASIC&socket_timeout=456");
      configurationBuilder.batchSize(1000);
      configuration = configurationBuilder.build();

      assertEquals(1, configuration.maxRetries());
      assertEquals(ClientIntelligence.BASIC, configuration.clientIntelligence());
      assertEquals(1000, configuration.batchSize());
      assertEquals(456, configuration.socketTimeout());
   }

   public void testMixOfConfiguration3() {
      Properties properties = new Properties();
      properties.setProperty(ConfigurationProperties.DEFAULT_EXECUTOR_FACTORY_POOL_SIZE, "1");
      properties.setProperty(ConfigurationProperties.URI, "hotrod://host1?client_intelligence=BASIC");
      ConfigurationBuilder cb = new ConfigurationBuilder().withProperties(properties);
      Configuration configuration = cb.build();
      assertEquals(ClientIntelligence.BASIC, configuration.clientIntelligence());
   }

   public void testConfigurationViaURI() {
      Configuration configuration = HotRodURI.create("hotrod://host1").toConfigurationBuilder().build();
      assertEquals(1, configuration.servers().size());
      assertFalse(configuration.security().ssl().enabled());
      assertFalse(configuration.security().authentication().enabled());
      configuration = HotRodURI.create("hotrod://host1?socket_timeout=5000&connect_timeout=1000").toConfigurationBuilder().build();
      assertEquals(1, configuration.servers().size());
      assertFalse(configuration.security().ssl().enabled());
      assertFalse(configuration.security().authentication().enabled());
      assertEquals(5000, configuration.socketTimeout());
      assertEquals(1000, configuration.connectionTimeout());
      configuration = HotRodURI.create("hotrod://host2:11322").toConfigurationBuilder().build();
      assertEquals(1, configuration.servers().size());
      assertEquals("host2", configuration.servers().get(0).host());
      assertEquals(11322, configuration.servers().get(0).port());
      assertFalse(configuration.security().ssl().enabled());
      assertFalse(configuration.security().authentication().enabled());
      configuration = HotRodURI.create("hotrod://user:password@host1:11222").toConfigurationBuilder().build();
      assertEquals(1, configuration.servers().size());
      assertFalse(configuration.security().ssl().enabled());
      assertTrue(configuration.security().authentication().enabled());
      BasicCallbackHandler callbackHandler = (BasicCallbackHandler) configuration.security().authentication().callbackHandler();
      assertEquals("user", callbackHandler.getUsername());
      assertArrayEquals("password".toCharArray(), callbackHandler.getPassword());
      configuration = HotRodURI.create("hotrod://host1:11222,host2:11322,host3").toConfigurationBuilder().build();
      assertEquals(3, configuration.servers().size());
      assertEquals("host1", configuration.servers().get(0).host());
      assertEquals(11222, configuration.servers().get(0).port());
      assertEquals("host2", configuration.servers().get(1).host());
      assertEquals(11322, configuration.servers().get(1).port());
      assertEquals("host3", configuration.servers().get(2).host());
      assertEquals(11222, configuration.servers().get(2).port());
      assertFalse(configuration.security().ssl().enabled());
      configuration = HotRodURI.create("hotrods://user:password@host1:11222,host2:11322?trust_store_path=cert.pem&sni_host_name=infinispan.test").toConfigurationBuilder().build();
      assertEquals(2, configuration.servers().size());
      assertEquals("host1", configuration.servers().get(0).host());
      assertEquals(11222, configuration.servers().get(0).port());
      assertEquals("host2", configuration.servers().get(1).host());
      assertEquals(11322, configuration.servers().get(1).port());
      assertTrue(configuration.security().ssl().enabled());
      assertTrue(configuration.security().authentication().enabled());
      callbackHandler = (BasicCallbackHandler) configuration.security().authentication().callbackHandler();
      assertEquals("user", callbackHandler.getUsername());
      assertArrayEquals("password".toCharArray(), callbackHandler.getPassword());
      expectException(IllegalArgumentException.class, "ISPN004095:.*", () -> HotRodURI.create("http://host1"));
      expectException(IllegalArgumentException.class, "ISPN004096:.*", () -> HotRodURI.create("hotrod://host1?property"));
   }

   public void testCacheNames() throws IOException {
      Properties properties = new Properties();
      try (InputStream is = this.getClass().getResourceAsStream("/hotrod-client-percache.properties")) {
         properties.load(is);
      }
      Configuration configuration = new ConfigurationBuilder().withProperties(properties).build();
      assertEquals(3, configuration.remoteCaches().size());
      assertTrue(configuration.remoteCaches().containsKey("mycache"));
      RemoteCacheConfiguration cache = configuration.remoteCaches().get("mycache");
      assertEquals("org.infinispan.DIST_SYNC", cache.templateName());
      assertTrue(cache.forceReturnValues());
      assertTrue(configuration.remoteCaches().containsKey("org.infinispan.yourcache"));
      cache = configuration.remoteCaches().get("org.infinispan.yourcache");
      assertEquals("org.infinispan.DIST_ASYNC", cache.templateName());
      assertEquals(NearCacheMode.INVALIDATED, cache.nearCacheMode());
      assertTrue(configuration.remoteCaches().containsKey("org.infinispan.*"));
      cache = configuration.remoteCaches().get("org.infinispan.*");
      assertEquals("org.infinispan.REPL_SYNC", cache.templateName());
      assertEquals(TransactionMode.NON_XA, cache.transactionMode());
   }

   @Test
   public void testPerCacheMarshallerConfig() throws IOException {
      Properties properties = new Properties();
      try (InputStream is = this.getClass().getResourceAsStream("/hotrod-client-percache.properties")) {
         properties.load(is);
      }
      Configuration configuration = new ConfigurationBuilder().withProperties(properties).build();

      assertEquals(JavaSerializationMarshaller.class, configuration.remoteCaches().get("mycache").marshallerClass());
      assertEquals(UTF8StringMarshaller.class, configuration.remoteCaches().get("org.infinispan.yourcache").marshallerClass());

      Properties props = configuration.properties();
      assertEquals(JavaSerializationMarshaller.class.getName(), props.getProperty("infinispan.client.hotrod.cache.mycache.marshaller"));
      assertEquals(UTF8StringMarshaller.class.getName(), props.getProperty("infinispan.client.hotrod.cache.org.infinispan.yourcache.marshaller"));
   }

   @Test
   public void testHotRodURItoString() {
      HotRodURI uri = HotRodURI.create("hotrod://user:secret@host1?client_intelligence=BASIC");
      assertEquals("hotrod://host1?client_intelligence=BASIC", uri.toString());
      assertEquals("hotrod://user:secret@host1?client_intelligence=BASIC", uri.toString(true));
   }
}
