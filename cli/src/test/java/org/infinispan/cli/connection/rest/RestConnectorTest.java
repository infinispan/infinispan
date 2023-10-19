package org.infinispan.cli.connection.rest;

import org.infinispan.cli.util.ZeroSecurityTrustManager;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/

public class RestConnectorTest {
   @Test
   public void testUrlWithCredentials() {
      RestConnector connector = new RestConnector();
      RestConnection connection = (RestConnection) connector.getConnection(new Properties(), "http://user:password@localhost:11222", null);
      RestClientConfigurationBuilder builder = connection.getBuilder();
      RestClientConfiguration configuration = builder.build();
      assertEquals(11222, configuration.servers().get(0).port());
      assertEquals("localhost", configuration.servers().get(0).host());
      assertTrue(configuration.security().authentication().enabled());
      assertEquals("user", configuration.security().authentication().username());
      assertArrayEquals("password".toCharArray(), configuration.security().authentication().password());
   }

   @Test
   public void testUrlWithoutCredentials() {
      RestConnector connector = new RestConnector();
      RestConnection connection = (RestConnection) connector.getConnection(new Properties(),"http://localhost:11222", null);
      RestClientConfigurationBuilder builder = connection.getBuilder();
      RestClientConfiguration configuration = builder.build();
      assertEquals(11222, configuration.servers().get(0).port());
      assertEquals("localhost", configuration.servers().get(0).host());
      assertFalse(configuration.security().authentication().enabled());
   }

   @Test
   public void testUrlWithoutPort() {
      RestConnector connector = new RestConnector();
      RestConnection connection = (RestConnection) connector.getConnection(new Properties(),"http://localhost", null);
      RestClientConfigurationBuilder builder = connection.getBuilder();
      RestClientConfiguration configuration = builder.build();
      assertEquals(80, configuration.servers().get(0).port());
      assertEquals("localhost", configuration.servers().get(0).host());
      assertFalse(configuration.security().authentication().enabled());
   }

   @Test
   public void testUrlWithSSL() throws NoSuchAlgorithmException {
      RestConnector connector = new RestConnector();
      RestConnection connection = (RestConnection) connector.getConnection(new Properties(),"https://localhost", null);
      RestClientConfigurationBuilder builder = connection.getBuilder();
      builder.security().ssl().sslContext(SSLContext.getDefault()).trustManagers(new TrustManager[]{new ZeroSecurityTrustManager()});
      RestClientConfiguration configuration = builder.build();
      assertEquals(443, configuration.servers().get(0).port());
      assertEquals("localhost", configuration.servers().get(0).host());
      assertFalse(configuration.security().authentication().enabled());
      assertTrue(configuration.security().ssl().enabled());
   }


   @Test
   public void testEmptyUrl() {
      RestConnector connector = new RestConnector();
      RestConnection connection = (RestConnection) connector.getConnection(new Properties(),"", null);
      RestClientConfigurationBuilder builder = connection.getBuilder();
      RestClientConfiguration configuration = builder.build();
      assertEquals(11222, configuration.servers().get(0).port());
      assertEquals("localhost", configuration.servers().get(0).host());
   }

   @Test
   public void testPlainHostPort() {
      RestConnector connector = new RestConnector();
      RestConnection connection = (RestConnection) connector.getConnection(new Properties(),"my.host.com:12345", null);
      RestClientConfigurationBuilder builder = connection.getBuilder();
      RestClientConfiguration configuration = builder.build();
      assertEquals(12345, configuration.servers().get(0).port());
      assertEquals("my.host.com", configuration.servers().get(0).host());
   }
}
