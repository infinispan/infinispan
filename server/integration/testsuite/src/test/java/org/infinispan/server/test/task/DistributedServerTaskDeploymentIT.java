package org.infinispan.server.test.task;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.server.test.category.Task;
import org.infinispan.server.test.task.servertask.DistributedDeploymentTestServerTask;
import org.infinispan.tasks.ServerTask;
import org.jboss.arquillian.container.test.api.Deployer;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.OverProtocol;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.junit.InSequence;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Testing the jar task deployment/undeploy.
 */
@RunWith(Arquillian.class)
@Category({Task.class})
@WithRunningServer({@RunningServer(name="clusteredcache-1"), @RunningServer(name = "clusteredcache-2")})
public class DistributedServerTaskDeploymentIT {

    @InfinispanResource("clusteredcache-1")
    RemoteInfinispanServer server1;

    @InfinispanResource("clusteredcache-2")
    RemoteInfinispanServer server2;

    @ArquillianResource
    private Deployer deployer;

    RemoteCacheManager rcm1;

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Deployment(name = "node1", managed = false)
    @TargetsContainer("clusteredcache-1")
    @OverProtocol("jmx-as7")
    public static JavaArchive create1() {
        JavaArchive jar = ShrinkWrap.create(JavaArchive.class, "custom-task-deployment.jar");
        jar.addClass(DistributedDeploymentTestServerTask.class);
        jar.addAsServiceProvider(ServerTask.class, DistributedDeploymentTestServerTask.class);

        return jar;
    }

    @Deployment(name = "node2", managed = false)
    @TargetsContainer("clusteredcache-2")
    @OverProtocol("jmx-as7")
    public static JavaArchive create2() {
        JavaArchive jar = ShrinkWrap.create(JavaArchive.class, "custom-task-deployment.jar");
        jar.addClass(DistributedDeploymentTestServerTask.class);
        jar.addAsServiceProvider(ServerTask.class, DistributedDeploymentTestServerTask.class);

        return jar;
    }

    @Before
    public void setUp() {
        if (rcm1 == null) {
            Configuration conf = new ConfigurationBuilder().addServer().host(server1.getHotrodEndpoint().getInetAddress().getHostName())
                    .port(server1.getHotrodEndpoint().getPort()).build();
            rcm1 = new RemoteCacheManager(conf);
        }
    }

    @Test
    @InSequence(1)
    public void testDeploy() {
        deployer.deploy("node1");
        deployer.deploy("node2");
    }

    @Test
    @InSequence(2)
    @SuppressWarnings("unchecked")
    public void shouldGatherNodeNamesInRemoteTasks() throws Exception {
        Object resultObject = rcm1.getCache().execute(DistributedDeploymentTestServerTask.NAME, Collections.emptyMap());
        assertNotNull(resultObject);
        List<String> result = (List<String>) resultObject;
        assertEquals(2, result.size());

        assertTrue("result list does not contain expected items.", result.containsAll(asList("node0", "node1")));
    }

    @Test
    @InSequence(3)
    public void testTaskUndeploy() {
        deployer.undeploy("node1");
        deployer.undeploy("node2");

        exceptionRule.expect(HotRodClientException.class);
        exceptionRule.expectMessage("ISPN027002");
        rcm1.getCache().execute(DistributedDeploymentTestServerTask.NAME, Collections.emptyMap());
    }
}