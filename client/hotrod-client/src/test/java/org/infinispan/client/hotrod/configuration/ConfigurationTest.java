package org.infinispan.client.hotrod.configuration;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.infinispan.client.hotrod.SomeAsyncExecutorFactory;
import org.infinispan.client.hotrod.SomeCustomConsistentHashV1;
import org.infinispan.client.hotrod.SomeRequestBalancingStrategy;
import org.infinispan.client.hotrod.SomeTransportfactory;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.transport.tcp.SaslTransportObjectFactory;
import org.infinispan.commons.CacheConfigurationException;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.configuration.ConfigurationTest", groups = "functional" )
public class ConfigurationTest {

   public void testConfiguration() {
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
         .consistentHashImpl(1, SomeCustomConsistentHashV1.class)
         .socketTimeout(100)
         .tcpNoDelay(false)
         .pingOnStartup(false)
         .keySizeEstimate(128)
         .valueSizeEstimate(1024)
         .maxRetries(0)
         .transportFactory(SomeTransportfactory.class);

      Configuration configuration = builder.build();
      validateConfiguration(configuration);

      ConfigurationBuilder newBuilder = new ConfigurationBuilder();
      newBuilder.read(configuration);
      Configuration newConfiguration = newBuilder.build();
      validateConfiguration(newConfiguration);
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

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "ISPN(\\d)*: Invalid max_retries \\(value=-1\\). " +
               "Value should be greater or equal than zero.")
   public void testNegativeRetriesPerServer() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.maxRetries(-1);
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
      builder.security().authentication().enable().saslMechanism("PLAIN").callbackHandler(SaslTransportObjectFactory.NoOpCallbackHandler.INSTANCE);
      builder.build();
   }

   private void assertServer(String host, int port, ServerConfiguration serverCfg) {
      assertEquals(host, serverCfg.host());
      assertEquals(port, serverCfg.port());
   }

   private void validateConfiguration(Configuration configuration) {
      assertEquals(2, configuration.servers().size());
      assertEquals(SomeAsyncExecutorFactory.class, configuration.asyncExecutorFactory().factoryClass());
      assertEquals(SomeRequestBalancingStrategy.class, configuration.balancingStrategy());
      assertEquals(SomeTransportfactory.class, configuration.transportFactory());
      assertEquals(SomeCustomConsistentHashV1.class, configuration.consistentHashImpl(1));
      assertEquals(100, configuration.connectionPool().maxActive());
      assertEquals(150, configuration.connectionPool().maxTotal());
      assertEquals(1000, configuration.connectionPool().maxWait());
      assertEquals(20, configuration.connectionPool().maxIdle());
      assertEquals(10, configuration.connectionPool().minIdle());
      assertEquals(ExhaustedAction.WAIT, configuration.connectionPool().exhaustedAction());
      assertEquals(5, configuration.connectionPool().numTestsPerEvictionRun());
      assertEquals(15000, configuration.connectionPool().timeBetweenEvictionRuns());
      assertEquals(12000, configuration.connectionPool().minEvictableIdleTime());
      assertTrue(configuration.connectionPool().testOnBorrow());
      assertTrue(configuration.connectionPool().testOnReturn());
      assertFalse(configuration.connectionPool().testWhileIdle());
      assertEquals(100, configuration.connectionTimeout());
      assertEquals(100, configuration.socketTimeout());
      assertFalse(configuration.tcpNoDelay());
      assertFalse(configuration.pingOnStartup());
      assertEquals(128, configuration.keySizeEstimate());
      assertEquals(1024, configuration.valueSizeEstimate());
      assertEquals(0, configuration.maxRetries());
   }

}
