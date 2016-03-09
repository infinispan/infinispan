package org.infinispan.server.test.task;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.server.test.category.Task;
import org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase;
import org.infinispan.server.test.task.servertask.DistributedAuthServerTask;
import org.infinispan.server.test.task.servertask.DistributedCacheUsingTask;
import org.infinispan.server.test.task.servertask.DistributedJSExecutingServerTask;
import org.infinispan.server.test.task.servertask.LocalAuthTestServerTask;
import org.infinispan.server.test.util.security.SaslConfigurationBuilder;
import org.infinispan.tasks.ServerTask;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests in distributed mode the server task execution in case if authentication is required.
 *
 * @author amanukya
 */
@RunWith(Arquillian.class)
@Category({Task.class})
@WithRunningServer({@RunningServer(name="hotrodAuthClustered"), @RunningServer(name = "hotrodAuthClustered-2")})
public class DistributedAuthServerTaskIT {
    @InfinispanResource("hotrodAuthClustered")
    RemoteInfinispanServer server1;

    @InfinispanResource("hotrodAuthClustered-2")
    RemoteInfinispanServer server2;

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @BeforeClass
    public static void before() throws Exception {
        String[] serverDirs = new String[]{System.getProperty("server1.dist"), System.getProperty("server2.dist")};

        JavaArchive jar = ShrinkWrap.create(JavaArchive.class);
        jar.addClass(DistributedAuthServerTask.class);
        jar.addClass(LocalAuthTestServerTask.class);
        jar.addAsServiceProvider(ServerTask.class, DistributedAuthServerTask.class, LocalAuthTestServerTask.class);

        for (String serverDir : serverDirs) {
            File f = new File(serverDir, "/standalone/deployments/custom-distributed-task-with-auth.jar");
            jar.as(ZipExporter.class).exportTo(f, true);
        }
    }

    @AfterClass
    public static void undeploy() {
        String serverDir = System.getProperty("server1.dist");
        File jar = new File(serverDir, "/standalone/deployments/custom-distributed-task-with-auth.jar");
        if (jar.exists())
            jar.delete();
        File f = new File(serverDir, "/standalone/deployments/custom-distributed-task-with-auth.jar.deployed");
        if (f.exists())
            f.delete();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldRunLocalAuthTest() throws Exception {
        SaslConfigurationBuilder config = new SaslConfigurationBuilder("DIGEST-MD5");
        config.forIspnServer(server1).withServerName("node0");
        config.forCredentials(HotRodSaslAuthTestBase.SUPERVISOR_LOGIN, HotRodSaslAuthTestBase.SUPERVISOR_PASSWD);
        RemoteCacheManager rcm = new RemoteCacheManager(config.build(), true);
        RemoteCache remoteCache = rcm.getCache(LocalAuthTestServerTask.CACHE_NAME);

        String result = (String) remoteCache.execute(LocalAuthTestServerTask.NAME, Collections.emptyMap());
        assertEquals(LocalAuthTestServerTask.EXECUTED_VALUE, result);
        assertEquals(true, remoteCache.get(LocalAuthTestServerTask.KEY));

        rcm.stop();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldRunDistAuthTest() throws Exception {
        SaslConfigurationBuilder config = new SaslConfigurationBuilder("DIGEST-MD5");
        config.forIspnServer(server1).withServerName("node0");
        config.forCredentials(HotRodSaslAuthTestBase.SUPERVISOR_LOGIN, HotRodSaslAuthTestBase.SUPERVISOR_PASSWD);
        RemoteCacheManager rcm = new RemoteCacheManager(config.build(), true);
        RemoteCache remoteCache = rcm.getCache(DistributedAuthServerTask.CACHE_NAME);

        List<String> result = (List<String>) remoteCache.execute(DistributedAuthServerTask.NAME, Collections.emptyMap());
        assertEquals(2, result.size());
        assertTrue("result list does not contain expected items.", result.containsAll(asList("node0", "node1")));

        rcm.stop();
    }

    @Test
    public void shouldThrowException() throws Exception {
        SaslConfigurationBuilder config = new SaslConfigurationBuilder("DIGEST-MD5");
        config.forIspnServer(server1).withServerName("node0");
        config.forCredentials(HotRodSaslAuthTestBase.ADMIN_LOGIN, HotRodSaslAuthTestBase.ADMIN_PASSWD);
        RemoteCacheManager rcm = new RemoteCacheManager(config.build(), true);
        RemoteCache remoteCache = rcm.getCache(DistributedAuthServerTask.CACHE_NAME);

        exceptionRule.expect(HotRodClientException.class);
        exceptionRule.expectMessage("lacks 'EXEC' permission");
        remoteCache.execute(DistributedAuthServerTask.NAME, Collections.emptyMap());

        rcm.stop();
    }
}
