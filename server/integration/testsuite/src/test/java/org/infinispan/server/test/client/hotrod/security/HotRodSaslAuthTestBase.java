package org.infinispan.server.test.client.hotrod.security;

import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testGetNonExistent;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testSize;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPut;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPutGet;

import java.security.PrivilegedActionException;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.server.test.util.security.SaslConfigurationBuilder;
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

   public static final String TEST_REALM = "ApplicationRealm";
   public static final String TEST_SERVER_NAME = "node0";

   public static final String TEST_CACHE_NAME = "testcache";
   public static final String TEST_KEY = "testKey";
   public static final String TEST_VALUE = "testValue";

   public static final String ADMIN_LOGIN = "admin";
   public static final String ADMIN_PASSWD = "strongPassword";
   public static final String READER_LOGIN = "reader";
   public static final String READER_PASSWD = "password";
   public static final String WRITER_LOGIN = "writer";
   public static final String WRITER_PASSWD = "somePassword";
   public static final String SUPERVISOR_LOGIN = "supervisor";
   public static final String SUPERVISOR_PASSWD = "lessStrongPassword";

   protected RemoteCache<String, String> remoteCache;
   protected static RemoteCacheManager remoteCacheManager = null;

   public abstract String getTestedMech();

   public abstract RemoteInfinispanServer getRemoteServer();

   public abstract void initAsAdmin() throws PrivilegedActionException, LoginException;

   public abstract void initAsReader() throws PrivilegedActionException, LoginException;

   public abstract void initAsWriter() throws PrivilegedActionException, LoginException;

   public abstract void initAsSupervisor() throws PrivilegedActionException, LoginException;

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

   protected void initializeOverSsl(String login, String password) {
      Configuration config = getRemoteCacheManagerOverSslConfig(login, password);
      remoteCacheManager = new RemoteCacheManager(config, true);
      remoteCache = remoteCacheManager.getCache(TEST_CACHE_NAME);
   }

   protected Configuration getRemoteCacheManagerConfig(String login, String password) {
      return getDefaultSaslConfigBuilder().forCredentials(login, password).build();
   }

   protected Configuration getRemoteCacheManagerOverSslConfig(String login, String password) {
      return getDefaultSaslConfigBuilder().forCredentials(login, password).withDefaultSsl().build();
   }

   protected Configuration getRemoteCacheManagerConfig(Subject subj) {
      return getDefaultSaslConfigBuilder().forSubject(subj).build();
   }

   protected SaslConfigurationBuilder getDefaultSaslConfigBuilder() {
      SaslConfigurationBuilder config = new SaslConfigurationBuilder(getTestedMech());
      config.forIspnServer(getRemoteServer()).withServerName(TEST_SERVER_NAME);
      return config;
   }

   @Test
   public void testAdmin() throws Exception {
      initAsAdmin();
      testPutGet(remoteCache);
      testSize(remoteCache);
   }

   @Test
   public void testSupervisor() throws Exception {
      initAsSupervisor();
      testPutGet(remoteCache);
      testSize(remoteCache);
   }

   @Test
   public void testWriter() throws Exception {
      initAsWriter();
      testPut(remoteCache);
   }

   @Test
   public void testReader() throws Exception {
      initAsReader();
      testGetNonExistent(remoteCache);
   }

   @Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
   public void testReaderWrite() throws PrivilegedActionException, LoginException {
      initAsReader();
      testPut(remoteCache);
   }

   @Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
   public void testWriterWriteRead() throws PrivilegedActionException, LoginException {
      initAsWriter();
      testPutGet(remoteCache);
   }

}
