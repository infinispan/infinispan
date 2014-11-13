package org.infinispan.server.test.client.hotrod.security;

import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.*;

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
      testPutGetBulk(remoteCache);
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
      testReplaceWitFlag(remoteCache);
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
      testPutGetBulk(remoteCache);
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
      testReplaceWitFlag(remoteCache);
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

   //ISPN-4977
   /*@Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
   public void testReaderBuldRead() throws PrivilegedActionException, LoginException {
      initAsReader();
      remoteCache.getBulk();
   }*/

   @Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
   public void testWriterClear() throws PrivilegedActionException, LoginException {
      initAsWriter();
      testClear(remoteCache);
   }

   @Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
   public void testWriterReplaceWithReturnFlag() throws PrivilegedActionException, LoginException {
      initAsWriter();
      testReplaceWitFlag(remoteCache);
   }

   @Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
   public void testReaderClear() throws PrivilegedActionException, LoginException {
      initAsReader();
      testClear(remoteCache);
   }

}
