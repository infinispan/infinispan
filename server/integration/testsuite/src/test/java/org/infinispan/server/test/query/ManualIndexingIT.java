package org.infinispan.server.test.query;

import java.util.List;

import javax.management.ObjectName;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.Search;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.server.infinispan.spi.InfinispanSubsystem;
import org.infinispan.server.test.category.Queries;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.infinispan.server.test.util.ITestUtils.SERVER1_MGMT_PORT;
import static org.junit.Assert.assertEquals;

/**
 * Tests manual indexing in server.
 *
 * @author anistor@redhat.com
 * @author vchepeli@redhat.com
 *
 */
@Category({ Queries.class })
@RunWith(Arquillian.class)
public class ManualIndexingIT extends RemoteQueryBaseIT {

    private static final String CACHE_CONTAINER_NAME = "clustered";
    private static final String CACHE_NAME = "localtestcache_manual";

    @InfinispanResource("remote-query")
    protected RemoteInfinispanServer server;

    private MBeanServerConnectionProvider jmxConnectionProvider;

    public ManualIndexingIT() {
        super(CACHE_CONTAINER_NAME, CACHE_NAME);
    }

    @Override
    public RemoteInfinispanServer getServer() {
        return server;
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
                .having("name").eq("Tom").toBuilder();

        User user = new User();
        user.setId(1);
        user.setName("Tom");
        user.setSurname("Cat");
        user.setGender(User.Gender.MALE);
        remoteCache.put(1, user);

        assertEquals(0, qb.build().list().size());

        //manual indexing
        ObjectName massIndexerName = new ObjectName("jboss." + InfinispanSubsystem.SUBSYSTEM_NAME + ":type=Query,manager="
                + ObjectName.quote(CACHE_CONTAINER_NAME)
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
