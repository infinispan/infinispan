package org.infinispan.server.test.client.hotrod.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.junit.After;
import org.junit.Test;

/**
 * 
 * Base class for tests of HotRod client SASL authentication. For supported SASL mechanisms see
 * {@code endpoint-7.0.xml} or later.
 * 
 * @author vjuranek
 * @since 7.0
 */
public abstract class HotRodSaslAuthTestBase {

   protected final String TEST_REALM = "ApplicationRealm";
   protected final String TEST_SERVER_NAME = "node0";

   protected final String TEST_CACHE_NAME = "testcache";
   protected final String TEST_KEY = "testKey";
   protected final String TEST_VALUE = "testValue";

   protected final String PASSWORD = "testpassword";
   protected final String ADMIN_LOGIN = "testadmin";
   protected final String READER_LOGIN = "testreader";
   protected final String WRITER_LOGIN = "testwriter";
   protected final String SUPERVISOR_LOGIN = "testsupervisor";

   protected RemoteCache<String, String> remoteCache;
   protected static RemoteCacheManager remoteCacheManager = null;

   @InfinispanResource("container1")
   RemoteInfinispanServer server;

   public abstract String getTestedMech();

   @After
   public void release() {
      if (remoteCacheManager != null) {
         remoteCacheManager.stop();
      }
   }

   public void initialize(String login, String password) {
      Configuration config = getRemoteCacheManagerConfig(login, password);
      remoteCacheManager = new RemoteCacheManager(config, true);
      remoteCache = remoteCacheManager.getCache(TEST_CACHE_NAME);
   }

   protected Configuration getRemoteCacheManagerConfig(String login, String password) {
      ConfigurationBuilder config = getDefaultConfigBuilder();
      config.security().authentication()
         .serverName(TEST_SERVER_NAME)
         .saslMechanism(getTestedMech())
         .callbackHandler(new LoginHandler(login, password, TEST_REALM))
         .enable();
      return config.build();
   }

   protected ConfigurationBuilder getDefaultConfigBuilder() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      for (RemoteInfinispanServer server : getServers()) {
         config.addServer().host(server.getHotrodEndpoint().getInetAddress().getHostName())
               .port(server.getHotrodEndpoint().getPort());
      }
      return config;
   }

   protected List<RemoteInfinispanServer> getServers() {
      List<RemoteInfinispanServer> servers = new ArrayList<RemoteInfinispanServer>();
      servers.add(server);
      return Collections.unmodifiableList(servers);
   }

   @Test
   public void testAdmin() throws IOException {
      initialize(ADMIN_LOGIN, PASSWORD);
      testWriteRead();
      testCreateCache();
   }

   @Test
   public void testReaderRead() throws IOException {
      initialize(READER_LOGIN, PASSWORD);
      testReadNonExitent();
   }

   @Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
   public void testReaderWrite() throws IOException {
      initialize(READER_LOGIN, PASSWORD);
      testWrite();
   }

   @Test
   public void testWriterWrite() throws IOException {
      initialize(WRITER_LOGIN, PASSWORD);
      testWrite();
   }

   @Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
   public void testWriterWriteRead() throws IOException {
      initialize(WRITER_LOGIN, PASSWORD);
      testWriteRead();
   }

   @Test
   public void testSupervisorWriteRead() throws IOException {
      initialize(SUPERVISOR_LOGIN, PASSWORD);
      testWriteRead();
   }

   protected void testCreateCache() {
      assertNotNull(remoteCacheManager.getCache("myNewCache"));
   }

   protected void testReadNonExitent() {
      assertEquals(null, remoteCache.get("nonExistentKey"));
   }

   protected void testRead() {
      assertTrue(remoteCache.containsKey(TEST_KEY));
      assertEquals(TEST_VALUE, remoteCache.get(TEST_KEY));
   }

   protected void testWrite() {
      assertNull(remoteCache.put(TEST_KEY, TEST_VALUE));
   }

   protected void testWriteRead() {
      testWrite();
      testRead();
   }

   public static class LoginHandler implements CallbackHandler {
      final private String login;
      final private String password;
      final private String realm;

      public LoginHandler(String login, String password) {
         this.login = login;
         this.password = password;
         this.realm = null;
      }

      public LoginHandler(String login, String password, String realm) {
         this.login = login;
         this.password = password;
         this.realm = realm;
      }

      @Override
      public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
         for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
               ((NameCallback) callback).setName(login);
            } else if (callback instanceof PasswordCallback) {
               ((PasswordCallback) callback).setPassword(password.toCharArray());
            } else if (callback instanceof RealmCallback) {
               ((RealmCallback) callback).setText(realm);
            } else {
               throw new UnsupportedCallbackException(callback);
            }
         }
      }
   }

}
