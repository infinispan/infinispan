package org.infinispan.server.test.security.cache;

import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testGetNonExistent;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPut;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.READER_LOGIN;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.TEST_CACHE_NAME;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.TEST_SERVER_NAME;

import java.security.PrivilegedActionException;
import javax.security.auth.login.LoginException;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.test.category.Security;
import org.infinispan.server.test.util.ClassRemoteCacheManager;
import org.infinispan.server.test.util.security.SecurityConfigurationHelper;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 *
 * ClusteredCacheAuthExternalIT test authentication and authorization with distributed cache and state transfer.
 * Test scenario is as follows:
 * 1. Start ISPN server
 * 2. Start second ISPN server and form cluster
 * 3. Authenticate via HR client to the first server via SSL and EXTERNAL SASL auth
 * 4. Shut down first server
 * 5. Do operation on remote cache via HR and verify it authorization works as expected. This remote operation
 *    happens on the second server.
 *
 * @author vjuranek
 * @since 9.0
 */
@RunWith(Arquillian.class)
@Category({ Security.class })
@WithRunningServer({@RunningServer(name="hotrodAuthExternalClustered-2")})
public class ClusteredCacheAuthExternalIT {

    private static final String SASL_MECH = "EXTERNAL";
    private static final String ARQ_NODE_1_ID = "hotrodAuthExternalClustered";

    @ArquillianResource
    static ContainerController controller;

    @InfinispanResource("hotrodAuthExternalClustered")
    static RemoteInfinispanServer server1;

    @InfinispanResource("hotrodAuthExternalClustered-2")
    static RemoteInfinispanServer server2;

   private RemoteCacheManager rcm;

   @ClassRule
   public static ClassRemoteCacheManager classRCM = new ClassRemoteCacheManager();

   @Before
   public void initRCM() throws Exception {
      rcm = classRCM.cache(() -> {
         controller.start(ARQ_NODE_1_ID);
         SecurityConfigurationHelper cb = new SecurityConfigurationHelper(SASL_MECH);
         cb.forIspnServer(server1)
           .withServerName(TEST_SERVER_NAME)
           .withDefaultSsl();
         cb.security().ssl().keyAlias("client1");
         cb.forExternalAuth();
         RemoteCacheManager remoteCacheManager = new RemoteCacheManager(cb.build(), true);
         controller.stop(ARQ_NODE_1_ID);
         return remoteCacheManager;
      });
   }

    private RemoteCache<String, String> getRemoteCacheFor(String login) {
       return rcm.getCache(TEST_CACHE_NAME);
    }

    @Test
    public void testReaderRead() throws PrivilegedActionException, LoginException {
        RemoteCache<String, String> cache = getRemoteCacheFor(READER_LOGIN);
        testGetNonExistent(cache);
    }

    @Test(expected = org.infinispan.client.hotrod.exceptions.HotRodClientException.class)
    public void testReaderWrite() throws PrivilegedActionException, LoginException {
        RemoteCache<String, String> cache = getRemoteCacheFor(READER_LOGIN);
        testPut(cache);
    }
}
