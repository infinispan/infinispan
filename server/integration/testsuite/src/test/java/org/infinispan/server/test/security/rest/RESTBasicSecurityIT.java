package org.infinispan.server.test.security.rest;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.server.test.client.rest.RESTHelper;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests BASIC security for REST endpoint as is configured via "auth-method" attribute on "rest-connector" element in
 * datagrid subsystem.
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 * @author <a href="mailto:vchepeli@redhat.com">Vitalii Chepeliuk</a>
 */
@RunWith(Arquillian.class)
@Ignore
public class RESTBasicSecurityIT extends AbstractBasicSecurity {

    private static final String CONTAINER = "rest-security-basic";
    @InfinispanResource("rest-security-basic")
    RemoteInfinispanServer server;

    @Before
    public void setUp() throws Exception {
        RESTHelper.addServer(server.getRESTEndpoint().getInetAddress().getHostName(), server.getRESTEndpoint()
                .getContextPath());
    }

    @After
    public void tearDown() throws Exception {
        RESTHelper.clearServers();
    }

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = "testsuite/rest-sec-basic-wr.xml")})
    public void testSecuredWriteOperations() throws Exception {
        securedWriteOperations();
    }

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = "testsuite/rest-sec-basic-rw.xml")})
    public void testSecuredReadWriteOperations() throws Exception {
        securedReadWriteOperations();
    }
}
