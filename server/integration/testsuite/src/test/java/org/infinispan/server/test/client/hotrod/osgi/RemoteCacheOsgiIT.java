package org.infinispan.server.test.client.hotrod.osgi;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.sampledomain.Address;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.AccountMarshaller;
import org.infinispan.protostream.sampledomain.marshallers.AddressMarshaller;
import org.infinispan.protostream.sampledomain.marshallers.GenderMarshaller;
import org.infinispan.protostream.sampledomain.marshallers.LimitsMarshaller;
import org.infinispan.protostream.sampledomain.marshallers.TransactionMarshaller;
import org.infinispan.protostream.sampledomain.marshallers.UserMarshaller;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.server.test.category.Osgi;
import org.infinispan.server.test.util.osgi.KarafTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.karaf.options.KarafDistributionOption;
import org.ops4j.pax.exam.options.RawUrlReference;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.ops4j.pax.exam.CoreOptions.maven;

/**
 * Simple test for RemoteCache running in OSGi (Karaf). Both basic put/get operations and remote queries are
 * tested.
 *
 * @author mgencur
 */
@RunWith(PaxExam.class)
@Category(Osgi.class)
@ExamReactorStrategy(PerClass.class)
public class RemoteCacheOsgiIT extends KarafTestSupport {

    private final String SERVER_HOST = "localhost";
    private final int HOTROD_PORT = 11222;
    private final String DEFAULT_CACHE = "notindexed";
    private final String INDEXED_CACHE = "testcache";
    private final String KARAF_VERSION = System.getProperty("version.karaf", "2.3.3");
    private final String RESOURCES_DIR = System.getProperty("resources.dir", System.getProperty("java.io.tmpdir"));
    private ConfigurationBuilder builder;
    private RemoteCacheManager manager;

    @Configuration
    public Option[] config() throws Exception {
        return new Option[]{
                KarafDistributionOption.karafDistributionConfiguration()
                        .frameworkUrl(maven().groupId("org.apache.karaf").artifactId("apache-karaf").type("zip").version(KARAF_VERSION))
                        .karafVersion(KARAF_VERSION),
                //install HotRod client feature ("feature" is a set of bundles, all the bundles are installed at once)
                KarafDistributionOption.features(maven().groupId("org.infinispan")
                        .artifactId("infinispan-client-hotrod").type("xml").classifier("features")
                        .versionAsInProject(), "hotrod-client-with-query"),
                KarafDistributionOption.features(new RawUrlReference("file:///" + RESOURCES_DIR.replace("\\", "/") + "/test-features.xml"), "query-sample-domain"),
                KarafDistributionOption.editConfigurationFileExtend("etc/jre.properties", "jre-1.7", "sun.misc"),
                KarafDistributionOption.editConfigurationFileExtend("etc/jre.properties", "jre-1.6", "sun.misc"),
                KarafDistributionOption.keepRuntimeFolder(),
        };
    }

    @Before
    public void setUp() {
        builder = new ConfigurationBuilder();
        builder.addServer()
                .host(SERVER_HOST)
                .port(HOTROD_PORT);
    }

    @After
    public void tearDown() {
        if (manager != null) {
            manager.stop();
        }
    }

    @Test
    public void testCustomObjectPutGet() {
        Person p = new Person("Martin");
        manager = new RemoteCacheManager(builder.build());
        RemoteCache<Object, Object> cache = manager.getCache(DEFAULT_CACHE);
        cache.put("k1", p);
        assertEquals(p, cache.get("k1"));
    }

    @Test
    public void testAttributeQuery() throws Exception {
        builder.marshaller(new ProtoStreamMarshaller());
        manager = new RemoteCacheManager(builder.build());
        RemoteCache<Integer, User> cache = manager.getCache(INDEXED_CACHE);

        SerializationContext ctx = ProtoStreamMarshaller.getSerializationContext(manager);

        URL resourceUrl = bundleContext.getBundle().getResource("/sample_bank_account/bank.protobin");
        ctx.registerProtofile(resourceUrl.openStream());

        ctx.registerMarshaller(new UserMarshaller());
        ctx.registerMarshaller(new GenderMarshaller());
        ctx.registerMarshaller(new AddressMarshaller());
        ctx.registerMarshaller(new AccountMarshaller());
        ctx.registerMarshaller(new LimitsMarshaller());
        ctx.registerMarshaller(new TransactionMarshaller());

        cache.put(1, createUser1());
        cache.put(2, createUser2());

        // get user back from remote cache and check its attributes
        User fromCache = cache.get(1);
        assertUser(fromCache);

        // get user back from remote cache via query and check its attributes
        QueryFactory qf = Search.getQueryFactory(cache);
        Query query = qf.from(User.class)
                .having("name").eq("Tom").toBuilder()
                .build();
        List<User> list = query.list();
        assertNotNull(list);
        assertEquals(1, list.size());
        assertEquals(User.class, list.get(0).getClass());
        assertUser(list.get(0));
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

    static class Person implements Serializable {

        final String name;

        public Person(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Person person = (Person) o;

            if (!name.equals(person.name)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

}