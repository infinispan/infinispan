package org.infinispan.server.test.query;

import java.util.Collections;
import java.util.List;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.Search;
import org.infinispan.protostream.sampledomain.Address;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for remote queries over HotRod on a local cache using RAM directory.
 *
 * @author Adrian Nistor
 * @author Martin Gencur
 */
@RunWith(Arquillian.class)
@WithRunningServer({@RunningServer(name = "remote-query")})
public class RemoteQueryIT extends RemoteQueryBaseIT {

    @InfinispanResource("remote-query")
    protected RemoteInfinispanServer server;

    public RemoteQueryIT() {
        super("local", "testcache");
    }

    protected RemoteQueryIT(String cacheContainerName, String cacheName) {
        super(cacheContainerName, cacheName);
    }

    @Override
    protected RemoteInfinispanServer getServer() {
        return server;
    }

    @Test
    public void testAttributeQuery() throws Exception {
        remoteCache.put(1, createUser1());
        remoteCache.put(2, createUser2());

        // get user back from remote cache and check its attributes
        User fromCache = remoteCache.get(1);
        assertUser(fromCache);

        // get user back from remote cache via query and check its attributes
        QueryFactory qf = Search.getQueryFactory(remoteCache);
        Query query = qf.from(User.class)
                .having("name").eq("Tom").toBuilder()
                .build();
        List<User> list = query.list();
        assertNotNull(list);
        assertEquals(1, list.size());
        assertEquals(User.class, list.get(0).getClass());
        assertUser(list.get(0));
    }

    @Test
    public void testEmbeddedAttributeQuery() throws Exception {
        remoteCache.put(1, createUser1());
        remoteCache.put(2, createUser2());

        // get user back from remote cache via query and check its attributes
        QueryFactory qf = Search.getQueryFactory(remoteCache);
        Query query = qf.from(User.class)
                .having("addresses.postCode").eq("1234").toBuilder()
                .build();
        List<User> list = query.list();
        assertNotNull(list);
        assertEquals(1, list.size());
        assertEquals(User.class, list.get(0).getClass());
        assertUser(list.get(0));
    }

    @Test
    public void testProjections() throws Exception {
        remoteCache.put(1, createUser1());
        remoteCache.put(2, createUser2());

        // get user back from remote cache and check its attributes
        User fromCache = remoteCache.get(1);
        assertUser(fromCache);

        // get user back from remote cache via query and check its attributes
        QueryFactory qf = Search.getQueryFactory(remoteCache);
        Query query = qf.from(User.class)
                .setProjection("name", "surname")
                .having("name").eq("Tom").toBuilder()
                .build();
        List<Object[]> list = query.list();
        assertNotNull(list);
        assertEquals(1, list.size());
        assertEquals(Object[].class, list.get(0).getClass());
        assertEquals("Tom", list.get(0)[0]);
        assertEquals("Cat", list.get(0)[1]);
    }

    private User createUser1() {
        User user = new User();
        user.setId(1);
        user.setName("Tom");
        user.setSurname("Cat");
        user.setGender(User.Gender.MALE);
        user.setAccountIds(Collections.singletonList(12));
        Address address = new Address();
        address.setStreet("Dark Alley");
        address.setPostCode("1234");
        user.setAddresses(Collections.singletonList(address));
        return user;
    }

    private User createUser2() {
        User user = new User();
        user.setId(1);
        user.setName("Adrian");
        user.setSurname("Nistor");
        user.setGender(User.Gender.MALE);
        Address address = new Address();
        address.setStreet("Old Street");
        address.setPostCode("XYZ");
        user.setAddresses(Collections.singletonList(address));
        return user;
    }

    private void assertUser(User user) {
        assertNotNull(user);
        assertEquals(1, user.getId());
        assertEquals("Tom", user.getName());
        assertEquals("Cat", user.getSurname());
        assertEquals(User.Gender.MALE, user.getGender());
        assertNotNull(user.getAccountIds());
        assertEquals(1, user.getAccountIds().size());
        assertEquals(12, user.getAccountIds().get(0).intValue());
        assertNotNull(user.getAddresses());
        assertEquals(1, user.getAddresses().size());
        assertEquals("Dark Alley", user.getAddresses().get(0).getStreet());
        assertEquals("1234", user.getAddresses().get(0).getPostCode());
    }
}
