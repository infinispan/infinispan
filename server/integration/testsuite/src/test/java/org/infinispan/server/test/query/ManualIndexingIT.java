package org.infinispan.server.test.query;

import static org.infinispan.server.test.util.ITestUtils.SERVER1_MGMT_PORT;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.ObjectName;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.Search;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.server.infinispan.spi.InfinispanSubsystem;
import org.infinispan.server.test.category.Queries;
import org.infinispan.server.test.category.SingleNode;
import org.infinispan.server.test.util.ManagementClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests manual indexing in server.
 *
 * @author anistor@redhat.com
 * @author vchepeli@redhat.com
 *
 */
@Category({SingleNode.class})
@RunWith(Arquillian.class)
public class ManualIndexingIT extends RemoteQueryBaseIT {

    private static final String CACHE_NAME = "localtestcache_manual";
    private static final String CACHE_TEMPLATE = "localtestcache_manualConfiguration";
    private static final String CACHE_CONTAINER = "local";

    @InfinispanResource("container1")
    protected RemoteInfinispanServer server;

    private MBeanServerConnectionProvider jmxConnectionProvider;

    public ManualIndexingIT() {
        super(CACHE_CONTAINER, CACHE_NAME);
    }

    @Override
    public RemoteInfinispanServer getServer() {
        server.reconnect();
        return server;
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        ManagementClient client = ManagementClient.getStandaloneInstance();
        client.addCacheConfiguration(CACHE_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
        Map<String, String> properties = new HashMap<>();
        properties.put("default.directory_provider", "ram");
        properties.put("lucene_version", "LUCENE_CURRENT");
        properties.put("hibernate.search.indexing_strategy", "manual");
        properties.put("hibernate.search.jmx_enabled", "true");
        client.enableIndexingForConfiguration(CACHE_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL, ManagementClient.IndexingType.LOCAL, properties);
        //Server reload required, see ISPN-7662
        client.reload();
        client.addCache(CACHE_NAME, CACHE_CONTAINER, CACHE_TEMPLATE, ManagementClient.CacheType.LOCAL);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        ManagementClient client = ManagementClient.getStandaloneInstance();
        client.removeCache(CACHE_NAME, CACHE_CONTAINER, ManagementClient.CacheType.LOCAL);
        client.removeCacheConfiguration(CACHE_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        jmxConnectionProvider = new MBeanServerConnectionProvider(getServer().getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT);
    }

    @Test
    public void testManualIndexing() throws Exception {
        QueryBuilder qb = Search.getQueryFactory(remoteCache).from(User.class)
                .having("name").eq("Tom");

        User user = new User();
        user.setId(1);
        user.setName("Tom");
        user.setSurname("Cat");
        user.setGender(User.Gender.MALE);
        remoteCache.put(1, user);

        assertEquals(0, qb.build().list().size());

        //manual indexing
        ObjectName massIndexerName = new ObjectName("jboss." + InfinispanSubsystem.SUBSYSTEM_NAME + ":type=Query,manager="
                + ObjectName.quote(CACHE_CONTAINER)
                + ",cache=" + ObjectName.quote(CACHE_NAME)
                + ",component=MassIndexer");
        jmxConnectionProvider.getConnection().invoke(massIndexerName, "start", null, null);

        List<User> list = qb.build().list();
        assertEquals(1, list.size());
        User foundUser = list.get(0);
        assertEquals(1, foundUser.getId());
        assertEquals("Tom", foundUser.getName());
        assertEquals("Cat", foundUser.getSurname());
        assertEquals(User.Gender.MALE, foundUser.getGender());
    }
}
