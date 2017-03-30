package org.infinispan.server.test.security.rest;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.SingleNode;
import org.infinispan.server.test.client.rest.RESTHelper;
import org.infinispan.server.test.util.ManagementClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests BASIC security for REST endpoint as is configured via "auth-method" attribute on "rest-connector" element in
 * datagrid subsystem.
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
 */
@RunWith(Arquillian.class)
@Category({SingleNode.class})
public class RESTBasicSecurityIT extends AbstractBasicSecurity {

    private static final String CACHE_TEMPLATE = "localCacheConfiguration";
    private static final String CACHE_CONTAINER = "local";
    private static final String REST_ENDPOINT = "rest-connector2";
    private static final String REST_NAMED_CACHE = "restNamedCache";
    private static final int REST_PORT = 8081;

    @InfinispanResource("container1") //rest-security-basic
    RemoteInfinispanServer server;

    @BeforeClass
    public static void beforeClass() throws Exception {
        ManagementClient client = ManagementClient.getStandaloneInstance();
        client.addLocalCache(REST_NAMED_CACHE, CACHE_CONTAINER, CACHE_TEMPLATE);
        client.addSocketBinding("rest2", "standard-sockets", REST_PORT);
        client.addRestEndpoint(REST_ENDPOINT, CACHE_CONTAINER, REST_NAMED_CACHE, "rest2");
        client.addRestAuthentication(REST_ENDPOINT, "ApplicationRealm", "BASIC");
        client.reloadIfRequired();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        ManagementClient client = ManagementClient.getInstance();
        client.removeRestEndpoint(REST_ENDPOINT);
        client.removeSocketBinding("rest2", "standard-sockets");
        client.removeCache(REST_NAMED_CACHE, CACHE_CONTAINER, ManagementClient.CacheType.LOCAL);
    }

    @Before
    public void setUp() throws Exception {
        server.reconnect();
        rest = new RESTHelper();
        rest.addServer(server.getRESTEndpoint().getInetAddress().getHostName(), REST_PORT, server.getRESTEndpoint()
                .getContextPath());
    }

    @After
    public void tearDown() throws Exception {
        rest.clearServers();
    }

    @Test
    public void testSecuredReadWriteOperations() throws Exception {
        securedReadWriteOperations();
    }
}
