package org.infinispan.server.test.security.cache;

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

import java.security.PrivilegedActionException;
import javax.security.auth.login.LoginException;

import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testGetNonExistent;
import static org.infinispan.server.test.client.hotrod.security.HotRodAuthzOperationTests.testPut;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.*;

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
public class ClusteredCacheAuthExteranlIT {

    private static final String SASL_MECH = "EXTERNAL";
    private static final String ARQ_NODE_1_ID = "hotrodAuthExternalClustered";

    @ArquillianResource
    public ContainerController controller;

    @InfinispanResource("hotrodAuthExternalClustered")
    RemoteInfinispanServer server1;

    @InfinispanResource("hotrodAuthExternalClustered-2")
    RemoteInfinispanServer server2;

    private static RemoteCacheManager rcm;
    private static boolean isInitialized = false; //Arquillian is not able to inject to static fields, so the ISPN server cannot be used in @BeforeClass method

    public void initRCM() {
        controller.start(ARQ_NODE_1_ID);
        final SaslConfigurationBuilder cb = new SaslConfigurationBuilder(SASL_MECH).forIspnServer(server1).withServerName(TEST_SERVER_NAME).withDefaultSsl();
        rcm = new RemoteCacheManager(cb.forExternalAuth().build(), true);
        controller.stop(ARQ_NODE_1_ID);
        isInitialized = true;
    }

    @AfterClass
    public static void release() {
        if(rcm != null) {
            rcm.stop();
        }
    }

    private synchronized RemoteCache<String, String> getRemoteCacheFor(String login) {
        if(!isInitialized) {
            initRCM();
        }
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
