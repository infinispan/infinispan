package org.infinispan.server.test.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.Search;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.protostream.sampledomain.Address;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.server.test.category.Queries;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for remote queries over HotRod on a local cache using RAM directory.
 *
 * @author Adrian Nistor
 * @author Martin Gencur
 */
@Category(Queries.class)
@RunWith(Arquillian.class)
public class RemoteQueryIT extends RemoteQueryBaseIT {

    @InfinispanResource("remote-query-1")
    protected RemoteInfinispanServer server;

    public RemoteQueryIT() {
        super("clustered", "localtestcache");
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
                .having("name").eq("Tom")
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
                .having("addresses.postCode").eq("1234")
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
                .select("name", "surname")
                .having("name").eq("Tom")
                .build();
        List<Object[]> list = query.list();
        assertNotNull(list);
        assertEquals(1, list.size());
        assertEquals(Object[].class, list.get(0).getClass());
        assertEquals("Tom", list.get(0)[0]);
        assertEquals("Cat", list.get(0)[1]);
    }

    /**
     * Sorting on a field that does not contain DocValues so Hibernate Search is forced to uninvert it.
     * @see <a href="https://issues.jboss.org/browse/ISPN-5729">https://issues.jboss.org/browse/ISPN-5729</a>
     */
    @Test
    public void testUninverting() throws Exception {
        remoteCache.put(1, createUser1());
        remoteCache.put(2, createUser2());

        QueryFactory qf = Search.getQueryFactory(remoteCache);
        Query query = qf.from(User.class)
              .having("name").eq("John")
              .orderBy("id")
              .build();
        assertEquals(0, query.list().size());
    }

    @Test
    public void testIteratorWithQuery() throws Exception {
        remoteCache.put(1, createUser1());
        remoteCache.put(2, createUser2());

        QueryFactory qf = Search.getQueryFactory(remoteCache);
        Query simpleQuery = qf.from(User.class).having("name").eq("Tom").build();

        List<Map.Entry<Object, Object>> entries = new ArrayList<>(1);
        try (CloseableIterator<Map.Entry<Object, Object>> iter = remoteCache.retrieveEntriesByQuery(simpleQuery, null, 3)) {
            while (iter.hasNext()) {
                entries.add(iter.next());
            }
        }
        assertEquals(1, entries.size());
        assertEquals("Cat", ((User) entries.get(0).getValue()).getSurname());
    }

    @Test
    public void testIteratorWithQueryAndProjections() throws Exception {
        remoteCache.put(1, createUser1());
        remoteCache.put(2, createUser2());

        QueryFactory qf = Search.getQueryFactory(remoteCache);
        Query simpleQuery = qf.from(User.class).select("surname", "name").having("name").eq("Tom").build();

        List<Map.Entry<Object, Object>> entries = new ArrayList<>(1);
        try (CloseableIterator<Map.Entry<Object, Object>> iter = remoteCache.retrieveEntriesByQuery(simpleQuery, null, 3)) {
            while (iter.hasNext()) {
                entries.add(iter.next());
            }
        }
        assertEquals(1, entries.size());
        Object[] projections = (Object[]) entries.get(0).getValue();
        assertEquals("Cat", projections[0]);
        assertEquals("Tom", projections[1]);
    }

    @Test
    public void testQueryViaRest() throws IOException {
        remoteCache.put(1, createUser1());
        remoteCache.put(2, createUser2());

        String query = "from sample_bank_account.User where name='Adrian'";

        String searchURI = "http://localhost:8080/rest/localtestcache?action=search&query=" + URLEncoder.encode(query, "UTF-8");
        HttpGet httpget = new HttpGet(searchURI);

        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             CloseableHttpResponse response = httpClient.execute(httpget)) {

            HttpEntity entity = response.getEntity();
            JsonNode results = new ObjectMapper().readTree(entity.getContent());
            assertEquals(1, results.get("total_results").asInt());
            EntityUtils.consume(entity);
        }
    }

    private User createUser1() {
        User user = new User();
        user.setId(1);
        user.setName("Tom");
        user.setSurname("Cat");
        user.setGender(User.Gender.MALE);
        user.setAccountIds(Collections.singleton(12));
        Address address = new Address();
        address.setStreet("Dark Alley");
        address.setPostCode("1234");
        user.setAddresses(Collections.singletonList(address));
        return user;
    }

    private User createUser2() {
        User user = new User();
        user.setId(2);
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
        assertTrue(user.getAccountIds().contains(12));
        assertNotNull(user.getAddresses());
        assertEquals(1, user.getAddresses().size());
        assertEquals("Dark Alley", user.getAddresses().get(0).getStreet());
        assertEquals("1234", user.getAddresses().get(0).getPostCode());
    }
}
