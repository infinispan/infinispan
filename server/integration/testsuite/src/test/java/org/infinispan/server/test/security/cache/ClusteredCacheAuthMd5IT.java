package org.infinispan.server.test.security.cache;

import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.ADMIN_LOGIN;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.ADMIN_PASSWD;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.READER_LOGIN;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.READER_PASSWD;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.SUPERVISOR_LOGIN;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.SUPERVISOR_PASSWD;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.TEST_CACHE_NAME;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.TEST_SERVER_NAME;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.WRITER_LOGIN;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.WRITER_PASSWD;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.testReadNonExistent;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.testSize;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.testWrite;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.testWriteRead;

import java.security.PrivilegedActionException;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.LoginException;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.test.category.Security;
import org.infinispan.server.test.util.security.SaslConfigurationBuilder;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * 
 * ClusteredCacheAuthMd5IT test authentication and authorization with distributed cache and state transfer.
 * Test scenario is as follows:
 * 1. Start ISPN server
 * 2. Start second ISPN server and form cluster
 * 3. Authenticate via HR client to the first server
 * 4. Shut down first server
 * 5. Do operation on remote cache via HR and verify it authorization works as expected. This remote operation
 *    happens on the second server. 
 * 
 * @author vjuranek
 * @since 7.0
 */
@RunWith(Arquillian.class)
@Category({ Security.class })
@WithRunningServer({@RunningServer(name="hotrodAuthClustered-2")})
public class ClusteredCacheAuthMd5IT {
   
   private static final String SASL_MECH = "DIGEST-MD5";
   private static final String ARQ_NODE_1_ID = "hotrodAuthClustered";

   @ArquillianResource
   public ContainerController controller;

   @InfinispanResource("hotrodAuthClustered")
   RemoteInfinispanServer server1;

   @InfinispanResource("hotrodAuthClustered-2")
   RemoteInfinispanServer server2;
   
   private static Map<String, RemoteCacheManager> rcms;
   private static boolean isInitialized = false; //Arquillian is not able to inject to static fields, so the ISPN server cannot be used in @BeforeClass method
   
   public void initRCMs() {
      controller.start(ARQ_NODE_1_ID);
      final SaslConfigurationBuilder cb = new SaslConfigurationBuilder(SASL_MECH).forIspnServer(server1).withServerName(TEST_SERVER_NAME);
      rcms = new HashMap<String, RemoteCacheManager>();
      rcms.put(ADMIN_LOGIN, new RemoteCacheManager(cb.forCredentials(ADMIN_LOGIN, ADMIN_PASSWD).build(), true));
      rcms.put(WRITER_LOGIN, new RemoteCacheManager(cb.forCredentials(WRITER_LOGIN, WRITER_PASSWD).build(), true));
      rcms.put(READER_LOGIN, new RemoteCacheManager(cb.forCredentials(READER_LOGIN, READER_PASSWD).build(), true));
      rcms.put(SUPERVISOR_LOGIN, new RemoteCacheManager(cb.forCredentials(SUPERVISOR_LOGIN, SUPERVISOR_PASSWD).build(), true));
      controller.stop(ARQ_NODE_1_ID);
      isInitialized = true;
   }
   
   @AfterClass
   public static void release() {
      for(String rcmKey : rcms.keySet()) {
         RemoteCacheManager rcm = rcms.get(rcmKey);
         if(rcm != null) {
            rcm.stop();
         }
      }
   }
   
   private synchronized RemoteCache<String, String> getRemoteCacheFor(String login) {
      if(!isInitialized) {
         initRCMs();
      }
      return rcms.get(login).getCache(TEST_CACHE_NAME);
   }
   
   @Test
   public void testAdmin() throws PrivilegedActionException, LoginException {
      RemoteCache<String, String> cache = getRemoteCacheFor(ADMIN_LOGIN);
      testWriteRead(cache);
      testSize(cache);
   }

   @Test
   public void testReaderRead() throws PrivilegedActionException, LoginException {
      RemoteCache<String, String> cache = getRemoteCacheFor(READER_LOGIN);
      testReadNonExistent(cache);
   }

   @Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
   public void testReaderWrite() throws PrivilegedActionException, LoginException {
      RemoteCache<String, String> cache = getRemoteCacheFor(READER_LOGIN);
      testWrite(cache);
   }

   @Test
   public void testWriterWrite() throws PrivilegedActionException, LoginException {
      RemoteCache<String, String> cache = getRemoteCacheFor(WRITER_LOGIN);
      testWrite(cache);
   }

   @Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
   public void testWriterWriteRead() throws PrivilegedActionException, LoginException {
      RemoteCache<String, String> cache = getRemoteCacheFor(WRITER_LOGIN);
      testWriteRead(cache);
   }

   @Test
   public void testSupervisorWriteRead() throws PrivilegedActionException, LoginException {
      RemoteCache<String, String> cache = getRemoteCacheFor(SUPERVISOR_LOGIN);
      testWriteRead(cache);
      testSize(cache);
   } 
   
}
