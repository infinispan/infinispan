package org.infinispan.server.test.task;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.test.category.Security;
import org.infinispan.server.test.category.Task;
import org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase;
import org.infinispan.server.test.task.servertask.LocalAuthTestServerTask;
import org.infinispan.server.test.util.security.SaslConfigurationBuilder;
import org.infinispan.tasks.ServerTask;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Collections;

/**
 * Tests in local mode the server task execution in case if authentication is required.
 *
 * @author amanukya
 */
@RunWith(Arquillian.class)
@Category({Task.class, Security.class})
@WithRunningServer({@RunningServer(name="hotrodAuth")})
@Ignore(value = "Is temporarily ignored until the issue ISPN-6251 is fixed.")
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

        File f = new File(serverDir, "/standalone/deployments/custom-task.jar");
        f.deleteOnExit();
        jar.as(ZipExporter.class).exportTo(f, true);
    }

    @Test
    public void shouldThrowAuthenticationException() throws Exception {
        SaslConfigurationBuilder config = new SaslConfigurationBuilder("DIGEST-MD5");
        config.forIspnServer(server).withServerName("node0");
        config.forCredentials(HotRodSaslAuthTestBase.SUPERVISOR_LOGIN, HotRodSaslAuthTestBase.SUPERVISOR_PASSWD);
        RemoteCacheManager rcm = new RemoteCacheManager(config.build(), true);
        RemoteCache remoteCache = rcm.getCache(LocalAuthTestServerTask.CACHE_NAME);

        remoteCache.execute(LocalAuthTestServerTask.NAME, Collections.emptyMap());
    }
}
