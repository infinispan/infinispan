package org.infinispan.server.test.client.hotrod.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.PrivilegedActionException;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.sasl.RealmCallback;

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

   protected static final String TEST_REALM = "ApplicationRealm";
   protected static final String TEST_SERVER_NAME = "node0";

   protected static final String TEST_CACHE_NAME = "testcache";
   protected static final String TEST_KEY = "testKey";
   protected static final String TEST_VALUE = "testValue";

   protected static final String ADMIN_LOGIN = "admin";
   protected static final String ADMIN_PASSWD = "strongPassword";
   protected static final String READER_LOGIN = "reader";
   protected static final String READER_PASSWD = "password";
   protected static final String WRITER_LOGIN = "writer";
   protected static final String WRITER_PASSWD = "somePassword";
   protected static final String SUPERVISOR_LOGIN = "supervisor";
   protected static final String SUPERVISOR_PASSWD = "lessStrongPassword";
   
   protected RemoteCache<String, String> remoteCache;
   protected static RemoteCacheManager remoteCacheManager = null;

   public abstract String getTestedMech();

   public abstract String getHRServerHostname();

   public abstract int getHRServerPort();

   public abstract void initAsAdmin() throws PrivilegedActionException,LoginException;

   public abstract void initAsReader() throws PrivilegedActionException,LoginException;

   public abstract void initAsWriter() throws PrivilegedActionException,LoginException;

   public abstract void initAsSupervisor() throws PrivilegedActionException,LoginException;

   @After
   public void release() {
      if (remoteCacheManager != null) {
         remoteCacheManager.stop();
      }
   }

   protected void initialize(Subject subj) throws PrivilegedActionException {
      final Configuration config = getRemoteCacheManagerConfig(subj);
      remoteCacheManager = new RemoteCacheManager(config, true);
      remoteCache = remoteCacheManager.getCache(TEST_CACHE_NAME);
   }

   protected void initialize(String login, String password) {
      Configuration config = getRemoteCacheManagerConfig(login, password);
      remoteCacheManager = new RemoteCacheManager(config, true);
      remoteCache = remoteCacheManager.getCache(TEST_CACHE_NAME);
   }

   protected Configuration getRemoteCacheManagerConfig(String login, String password) {
      ConfigurationBuilder config = getDefaultConfigBuilder();
      config.security().authentication().callbackHandler(new LoginHandler(login, password, TEST_REALM));
      return config.build();
   }

   protected Configuration getRemoteCacheManagerConfig(Subject subj) {
      ConfigurationBuilder config = getDefaultConfigBuilder();
      config.security().authentication().clientSubject(subj).callbackHandler(new LoginHandler("", "")); //callback handle is required by ISPN config validation
      return config.build();
   }

   protected ConfigurationBuilder getDefaultConfigBuilder() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.addServer().host(getHRServerHostname()).port(getHRServerPort());
      config.security().authentication().serverName(TEST_SERVER_NAME).saslMechanism(getTestedMech()).enable();
      return config;
   }

   @Test
   public void testAdmin() throws PrivilegedActionException,LoginException {
      initAsAdmin();
      testWriteRead();
      testCreateCache();
   }

   @Test
   public void testReaderRead() throws PrivilegedActionException,LoginException {
      initAsReader();
      testReadNonExitent();
   }

   @Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
   public void testReaderWrite() throws PrivilegedActionException,LoginException { 
      initAsReader();
      testWrite();
   }

   @Test
   public void testWriterWrite() throws PrivilegedActionException,LoginException {
      initAsWriter();
      testWrite();
   }

   @Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
   public void testWriterWriteRead() throws PrivilegedActionException,LoginException {
      initAsWriter();
      testWriteRead();
   }

   @Test
   public void testSupervisorWriteRead() throws PrivilegedActionException,LoginException {
      initAsSupervisor();
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
