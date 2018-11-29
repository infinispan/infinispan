package org.infinispan.server.test.client.hotrod.security;

import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testAddGetClientListener;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testClear;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testContainsKey;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testGetNonExistent;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testGetNonExistentAsync;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testGetRemoteCacheManager;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testGetVersioned;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testGetWithMetadata;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPut;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPutAll;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPutAllAsync;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPutAsync;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPutClear;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPutClearAsync;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPutContainsKey;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPutGet;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPutGetAsync;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPutGetVersioned;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPutGetWithMetadata;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPutIfAbsent;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPutIfAbsentAsync;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testRemove;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testRemoveAsync;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testRemoveClientListener;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testRemoveContains;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testRemoveContainsAsync;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testRemoveWithVersion;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testRemoveWithVersionAsync;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testReplaceWithFlag;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testReplaceWithVersionAsync;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testReplaceWithVersioned;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testSize;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testStats;

import java.security.PrivilegedActionException;

import javax.security.auth.login.LoginException;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.server.test.category.Security;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 *
 * Hot Rod ({@link RemoteCache} ) authorization tests
 *
 * @author vjuranek
 * @since 7.0
 */
@RunWith(Arquillian.class)
@Category({ Security.class })
@WithRunningServer({ @RunningServer(name = "hotrodAuthClustered"), @RunningServer(name = "hotrodAuthClustered-2") })
public class HotRodOperationsAuthzIT extends HotRodSaslAuthTestBase {

   @InfinispanResource("hotrodAuthClustered")
   RemoteInfinispanServer server1;

   @InfinispanResource("hotrodAuthClustered-2")
   RemoteInfinispanServer server2;

   @Override
   public String getTestedMech() {
      return "DIGEST-MD5";
   }

   @Override
   public RemoteInfinispanServer getRemoteServer() {
      return server1;
   }

   @Override
   public void initAsAdmin() {
      initialize(ADMIN_LOGIN, ADMIN_PASSWD);
   }

   @Override
   public void initAsReader() {
      initialize(READER_LOGIN, READER_PASSWD);
   }

   @Override
   public void initAsWriter() {
      initialize(WRITER_LOGIN, WRITER_PASSWD);
   }

   @Override
   public void initAsSupervisor() {
      initialize(SUPERVISOR_LOGIN, SUPERVISOR_PASSWD);
   }

   @Test
   @Override
   public void testAdmin() throws Exception {
      initAsAdmin();
      testGetRemoteCacheManager(remoteCache);
      testPutClear(remoteCache);
      testPutClearAsync(remoteCache);
      testPutContainsKey(remoteCache);
      testPutGet(remoteCache);
      testPutGetAsync(remoteCache);
      testPutGetVersioned(remoteCache);
      testPutGetWithMetadata(remoteCache);
      //testKeySet(remoteCache); //ISPN-4977
      testPutAll(remoteCache);
      testPutAllAsync(remoteCache);
      testPutIfAbsent(remoteCache);
      testPutIfAbsentAsync(remoteCache);
      testRemoveContains(remoteCache);
      testRemoveContainsAsync(remoteCache);
      testRemoveWithVersion(remoteCache);
      testRemoveWithVersionAsync(remoteCache);
      testReplaceWithFlag(remoteCache);
      testReplaceWithVersioned(remoteCache);
      testReplaceWithVersionAsync(remoteCache);
      testSize(remoteCache);
      testStats(remoteCache);
      testAddGetClientListener(remoteCache);
      testRemoveClientListener(remoteCache);
   }

   @Test
   @Override
   public void testSupervisor() throws Exception {
      initAsSupervisor();
      testGetRemoteCacheManager(remoteCache);
      testPutClear(remoteCache);
      testPutClearAsync(remoteCache);
      testPutContainsKey(remoteCache);
      testPutGet(remoteCache);
      testPutGetAsync(remoteCache);
      testPutGetVersioned(remoteCache);
      testPutGetWithMetadata(remoteCache);
      //testKeySet(remoteCache); //ISPN-4977
      testPutAll(remoteCache);
      testPutAllAsync(remoteCache);
      testPutIfAbsent(remoteCache);
      testPutIfAbsentAsync(remoteCache);
      testRemoveContains(remoteCache);
      testRemoveContainsAsync(remoteCache);
      testRemoveWithVersion(remoteCache);
      testRemoveWithVersionAsync(remoteCache);
      testReplaceWithFlag(remoteCache);
      testReplaceWithVersioned(remoteCache);
      testReplaceWithVersionAsync(remoteCache);
      testSize(remoteCache);
   }

   @Test
   @Override
   public void testWriter() throws Exception {
      initAsWriter();
      testPut(remoteCache);
      testPutAsync(remoteCache);
      testRemove(remoteCache);
      testRemoveAsync(remoteCache);
   }

   @Test
   @Override
   public void testReader() throws Exception {
      initAsReader();
      testContainsKey(remoteCache);
      testGetNonExistent(remoteCache);
      testGetNonExistentAsync(remoteCache);
      testGetVersioned(remoteCache);
      testGetWithMetadata(remoteCache);
   }

   @Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
   public void testSupervisorStats() throws PrivilegedActionException, LoginException {
      initAsSupervisor();
      testStats(remoteCache);
   }

   @Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
   public void testSupervisorAddListener() throws PrivilegedActionException, LoginException {
      initAsSupervisor();
      testAddGetClientListener(remoteCache);
   }

   @Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
   public void testWriterClear() throws PrivilegedActionException, LoginException {
      initAsWriter();
      testClear(remoteCache);
   }

   @Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
   public void testWriterReplaceWithReturnFlag() throws PrivilegedActionException, LoginException {
      initAsWriter();
      testReplaceWithFlag(remoteCache);
   }

   @Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
   public void testReaderClear() throws PrivilegedActionException, LoginException {
      initAsReader();
      testClear(remoteCache);
   }

}
