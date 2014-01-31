package org.infinispan.server.test.rest.security;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.client.rest.RESTHelper;
import org.jboss.arquillian.container.test.api.Config;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.junit.InSequence;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.servlet.http.HttpServletResponse;

import static org.infinispan.server.test.client.rest.RESTHelper.*;

/**
 * Tests BASIC security for REST endpoint as is configured via "auth-method" attribute on "rest-connector" element in
 * datagrid subsystem.
 *
 * @author Martin Gencur
 */
@RunWith(Arquillian.class)
public class RESTBasicSecurityTest {

    private static final String SERVER_CONFIG_PROPERTY = "serverConfig";
    //configuration file with READ_WRITE security
    private static final String CONFIG_READ_WRITE_SECURED = "testsuite/rest-sec-basic-rw.xml";
    private static final String TEST_USER_NAME = "testuser";
    private static final String TEST_USER_PASSWORD = "testpassword";
    private static final String KEY_D = "d";
    private static final String CONTAINER1 = "rest-security-basic";

    @InfinispanResource("rest-security-basic")
    RemoteInfinispanServer server1;

    @ArquillianResource
    ContainerController controller;

    @Test
    @InSequence(1)
    public void testSecuredWriteOperations() throws Exception {
        try {
            controller.start(CONTAINER1);
            RESTHelper.addServer(server1.getRESTEndpoint().getInetAddress().getHostName(), server1.getRESTEndpoint()
                    .getContextPath());
            RESTHelper.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
            put(fullPathKey(KEY_A), "data", "application/text", HttpServletResponse.SC_OK);
            RESTHelper.clearCredentials();
            put(fullPathKey(KEY_B), "data", "application/text", HttpServletResponse.SC_UNAUTHORIZED);
            RESTHelper.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
            post(fullPathKey(KEY_C), "data", "application/text", HttpServletResponse.SC_OK);
            RESTHelper.clearCredentials();
            post(fullPathKey(KEY_D), "data", "application/text", HttpServletResponse.SC_UNAUTHORIZED);
            get(fullPathKey(KEY_A), "data");
            head(fullPathKey(KEY_A), HttpServletResponse.SC_OK);
            delete(fullPathKey(KEY_A), HttpServletResponse.SC_UNAUTHORIZED);
            RESTHelper.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
            delete(fullPathKey(KEY_A), HttpServletResponse.SC_OK);
            delete(fullPathKey(KEY_C), HttpServletResponse.SC_OK);
            RESTHelper.clearCredentials();
        } finally {
            controller.stop(CONTAINER1);
        }
    }

    @Test
    @InSequence(2)
    public void testSecuredReadWriteOperations() throws Exception {
        try {
            controller.start(CONTAINER1, new Config().add(SERVER_CONFIG_PROPERTY, CONFIG_READ_WRITE_SECURED).map());
            RESTHelper.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
            put(fullPathKey(KEY_A), "data", "application/text", HttpServletResponse.SC_OK);
            RESTHelper.clearCredentials();
            put(fullPathKey(KEY_B), "data", "application/text", HttpServletResponse.SC_UNAUTHORIZED);
            RESTHelper.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
            post(fullPathKey(KEY_C), "data", "application/text", HttpServletResponse.SC_OK);
            RESTHelper.clearCredentials();
            post(fullPathKey(KEY_D), "data", "application/text", HttpServletResponse.SC_UNAUTHORIZED);
            get(fullPathKey(KEY_A), HttpServletResponse.SC_UNAUTHORIZED);
            RESTHelper.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
            get(fullPathKey(KEY_A), "data");
            RESTHelper.clearCredentials();
            head(fullPathKey(KEY_A), HttpServletResponse.SC_UNAUTHORIZED);
            RESTHelper.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
            head(fullPathKey(KEY_A), HttpServletResponse.SC_OK);
            RESTHelper.clearCredentials();
            delete(fullPathKey(KEY_A), HttpServletResponse.SC_UNAUTHORIZED);
            RESTHelper.setCredentials(TEST_USER_NAME, TEST_USER_PASSWORD);
            delete(fullPathKey(KEY_A), HttpServletResponse.SC_OK);
            delete(fullPathKey(KEY_C), HttpServletResponse.SC_OK);
            RESTHelper.clearCredentials();
        } finally {
            controller.stop(CONTAINER1);
        }
    }
}
