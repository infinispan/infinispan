package org.infinispan.server.test.task;

import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.ADMIN_LOGIN;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.ADMIN_PASSWD;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.EXECUTOR_LOGIN;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.EXECUTOR_PASSWORD;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Collections;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.server.test.category.Security;
import org.infinispan.server.test.category.Task;
import org.infinispan.server.test.task.servertask.LocalAuthTestServerTask;
import org.infinispan.server.test.util.security.SaslConfigurationBuilder;
import org.infinispan.tasks.ServerTask;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

/**
 * Tests in local mode the server task execution in case if authentication is required.
 *
 * @author amanukya
 */
@RunWith(Arquillian.class)
@Category({Task.class, Security.class})
@WithRunningServer({@RunningServer(name="hotrodAuth")})
public class LocalAuthServerTaskIT {
    @InfinispanResource("hotrodAuth")
    RemoteInfinispanServer server;

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @BeforeClass
    public static void before() throws Exception {
        String serverDir = System.getProperty("server1.dist");

        JavaArchive jar = ShrinkWrap.create(JavaArchive.class);
        jar.addClass(LocalAuthTestServerTask.class);
        jar.addAsServiceProvider(ServerTask.class, LocalAuthTestServerTask.class);

        File f = new File(serverDir, "/standalone/deployments/custom-task-auth.jar");
        jar.as(ZipExporter.class).exportTo(f, true);
    }

    @AfterClass
    public static void undeploy() {
        String serverDir = System.getProperty("server1.dist");
        File jar = new File(serverDir, "/standalone/deployments/custom-task-auth.jar");
        if (jar.exists())
            jar.delete();

        File f = new File(serverDir, "/standalone/deployments/custom-task-auth.jar.deployed");
        if (f.exists())
            f.delete();
    }

    @Test
    public void shouldPassWithAuth() throws Exception {
        SaslConfigurationBuilder config = new SaslConfigurationBuilder("DIGEST-MD5");
        config.forIspnServer(server).withServerName("node0");
        config.forCredentials(EXECUTOR_LOGIN, EXECUTOR_PASSWORD);
        RemoteCacheManager rcm = new RemoteCacheManager(config.build(), true);
        RemoteCache remoteCache = rcm.getCache(LocalAuthTestServerTask.CACHE_NAME);

        String result = (String) remoteCache.execute(LocalAuthTestServerTask.NAME, Collections.emptyMap());
        assertEquals(LocalAuthTestServerTask.EXECUTED_VALUE, result);
        assertEquals(true, remoteCache.get(LocalAuthTestServerTask.KEY));
    }

    @Test
    public void shouldThrowException() throws Exception {
        SaslConfigurationBuilder config = new SaslConfigurationBuilder("DIGEST-MD5");
        config.forIspnServer(server).withServerName("node0");
        config.forCredentials(ADMIN_LOGIN, ADMIN_PASSWD);
        RemoteCacheManager rcm = new RemoteCacheManager(config.build(), true);
        RemoteCache remoteCache = rcm.getCache(LocalAuthTestServerTask.CACHE_NAME);

        exceptionRule.expect(HotRodClientException.class);
        exceptionRule.expectMessage("lacks 'EXEC' permission");
        remoteCache.execute(LocalAuthTestServerTask.NAME, Collections.emptyMap());
    }
}
