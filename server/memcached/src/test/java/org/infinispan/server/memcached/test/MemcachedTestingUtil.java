package org.infinispan.server.memcached.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;
import javax.security.auth.Subject;
import javax.security.sasl.Sasl;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.test.security.TestCertificates;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.server.core.security.simple.SimpleSaslAuthenticator;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.infinispan.server.memcached.configuration.MemcachedServerConfiguration;
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder;
import org.infinispan.server.memcached.logging.Log;

import net.spy.memcached.ClientMode;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;


/**
 * Utils for Memcached tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class MemcachedTestingUtil {
   private static final Log log = LogFactory.getLog(MemcachedTestingUtil.class, Log.class);

   private static final String host = "127.0.0.1";
   private static final String USERNAME = "user";
   private static final String REALM = "default";
   private static final String PASSWORD = "secret";
   private static final long TIMEOUT = 10_000;

   public static MemcachedClient createMemcachedClient(MemcachedServer server) throws IOException {
      MemcachedServerConfiguration configuration = server.getConfiguration();
      MemcachedProtocol protocol = configuration.protocol();
      ConnectionFactoryBuilder.Protocol p = protocol == MemcachedProtocol.BINARY ? ConnectionFactoryBuilder.Protocol.BINARY : ConnectionFactoryBuilder.Protocol.TEXT;
      ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder().setProtocol(p).setOpTimeout(TIMEOUT);
      if (configuration.authentication().enabled()) { // && (protocol == MemcachedProtocol.BINARY)) {
         builder.setAuthDescriptor(AuthDescriptor.typical(USERNAME, PASSWORD));
      }
      if (configuration.ssl().enabled()) {
         SSLContext sslContext = new SslContextFactory().trustStoreFileName(TestCertificates.certificate("ca")).trustStorePassword(TestCertificates.KEY_PASSWORD).build().sslContext();
         builder.setSSLContext(sslContext).setSkipTlsHostnameVerification(true);
      }
      builder.setClientMode(ClientMode.Static); // Legacy memcached mode

      return new MemcachedClient(builder.build(), Collections.singletonList(new InetSocketAddress(host, server.getPort())));
   }

   public static MemcachedServerConfigurationBuilder serverBuilder() {
      String serverName = TestResourceTracker.getCurrentTestShortName();
      return new MemcachedServerConfigurationBuilder().name(serverName).host(host).port(UniquePortThreadLocal.INSTANCE.get()).clientEncoding(MediaType.APPLICATION_OCTET_STREAM);
   }

   public static MemcachedServerConfigurationBuilder enableAuthentication(MemcachedServerConfigurationBuilder builder) {
      SimpleSaslAuthenticator ssap = new SimpleSaslAuthenticator();
      ssap.addUser(USERNAME, REALM, PASSWORD.toCharArray());
      builder.authentication().enable().sasl().addAllowedMech("CRAM-MD5").authenticator(ssap)
            .serverName("localhost").addMechProperty(Sasl.POLICY_NOANONYMOUS, "true");
      builder.authentication().text().authenticator((username, password) -> {
         if (username.equals(USERNAME) && new String(password).equals(PASSWORD)) {
            return CompletableFuture.completedFuture(new Subject());
         }
         return CompletableFuture.failedFuture(new SecurityException());
      });
      return builder;
   }

   public static MemcachedServerConfigurationBuilder enableEncryption(MemcachedServerConfigurationBuilder builder) {
      builder.ssl().enable()
            .keyStoreFileName(TestCertificates.certificate("server"))
            .keyStorePassword(TestCertificates.KEY_PASSWORD);
      return builder;
   }

   public static void killMemcachedClient(MemcachedClient client) {
      try {
         if (client != null) client.shutdown();
      } catch (Throwable t) {
         log.error("Error stopping client", t);
      }
   }

   public static void killMemcachedServer(MemcachedServer server) {
      if (server != null) server.stop();
   }

   private static final class UniquePortThreadLocal extends ThreadLocal<Integer> {

      static UniquePortThreadLocal INSTANCE = new UniquePortThreadLocal();

      private static final AtomicInteger UNIQUE_ADDR = new AtomicInteger(16211);

      @Override
      protected Integer initialValue() {
         return UNIQUE_ADDR.getAndAdd(100);
      }
   }
}
