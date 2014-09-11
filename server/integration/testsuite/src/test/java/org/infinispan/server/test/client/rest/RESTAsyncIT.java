package org.infinispan.server.test.client.rest;

import java.net.URI;

import javax.servlet.http.HttpServletResponse;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.RESTClustered;
import org.infinispan.server.test.util.ITestUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.infinispan.server.test.client.rest.RESTHelper.KEY_A;
import static org.infinispan.server.test.client.rest.RESTHelper.KEY_B;
import static org.infinispan.server.test.client.rest.RESTHelper.delete;
import static org.infinispan.server.test.client.rest.RESTHelper.fullPathKey;
import static org.infinispan.server.test.client.rest.RESTHelper.get;
import static org.infinispan.server.test.client.rest.RESTHelper.getWithoutAssert;
import static org.infinispan.server.test.client.rest.RESTHelper.head;
import static org.infinispan.server.test.client.rest.RESTHelper.put;
import static org.infinispan.server.test.util.ITestUtils.eventually;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the REST client putAsync header.
 *
 * @author <a href="mailto:jvilkola@redhat.com">Jozef Vilkolak</a>
 * @version October 2011
 */
@RunWith(Arquillian.class)
@Category({ RESTClustered.class })
public class RESTAsyncIT {

    @InfinispanResource("container1")
    RemoteInfinispanServer server1;

    @InfinispanResource("container2")
    RemoteInfinispanServer server2;

    @Before
    public void setUp() throws Exception {
        RESTHelper.addServer(server1.getRESTEndpoint().getInetAddress().getHostName(), server1.getRESTEndpoint().getContextPath());
        RESTHelper.addServer(server2.getRESTEndpoint().getInetAddress().getHostName(), server2.getRESTEndpoint().getContextPath());

        delete(fullPathKey(KEY_A));
        head(fullPathKey(KEY_A), HttpServletResponse.SC_NOT_FOUND);
    }

    @After
    public void tearDown() throws Exception {
        delete(fullPathKey(null));
    }

    @Test
    public void testPutOperation() throws Exception {
        URI fullPathKey = fullPathKey(KEY_A);
        int NUM_OPERATIONS = 1000;
        String initialXML = "<hey>ho</hey>";

        StringBuilder initial = new StringBuilder(initialXML);
        for (int i = 0; i < 200; i++) {
            initial.append("expanding");
        }
        initialXML = initial.toString();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < NUM_OPERATIONS; i++) {
            put(fullPathKey, initialXML, "application/octet-stream", HttpServletResponse.SC_OK, "performAsync", "false");
        }
        long putSyncTime = System.currentTimeMillis() - t1;

        delete(fullPathKey);

        t1 = System.currentTimeMillis();
        for (int i = 0; i < NUM_OPERATIONS; i++) {
            put(fullPathKey, initialXML, "application/octet-stream", HttpServletResponse.SC_OK, "performAsync", "true");
        }
        long putAsyncTime = System.currentTimeMillis() - t1;

        assertTrue("PUT : async- " + putAsyncTime + ", sync- " + putSyncTime, putAsyncTime < putSyncTime);
        get(fullPathKey, initialXML, HttpServletResponse.SC_OK, true, "performAsync", "true");
    }

    @Test
    public void testDeleteOperation() throws Exception {
        int NUM_OPERATIONS = 15;

        int SIZE = 900;
        byte[] bytes = new byte[SIZE];
        for (int i = 0; i < SIZE; i++) {
            bytes[i] = (byte) (i % 10);
        }

        for (int i = 0; i < NUM_OPERATIONS; i++) {
            put(fullPathKey(String.valueOf(i)), bytes, "application/octet-stream", HttpServletResponse.SC_OK, "performAsync", "false");
        }

        for (int i = 0; i < NUM_OPERATIONS; i++) {
            delete(fullPathKey(String.valueOf(i)), HttpServletResponse.SC_OK, "performAsync", "true");
        }

        for (int i = 0; i < NUM_OPERATIONS; i++) {
            final int iter = i; // inner class can access only final variables...
            // since the delete is async, the get can execute before the delete finishes (delete needs communication between nodes)
            eventually(new ITestUtils.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return getWithoutAssert(fullPathKey(String.valueOf(iter)), null, HttpServletResponse.SC_NOT_FOUND, true, "performAsync", "true");
                }
            }, 5000, 10);
        }

        put(fullPathKey(KEY_A), KEY_A, "application/octet-stream");
        put(fullPathKey(KEY_B), KEY_B, "application/octet-stream");
        delete(fullPathKey(null), HttpServletResponse.SC_OK, "performAsync", "true");

        eventually(new ITestUtils.Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return getWithoutAssert(fullPathKey(KEY_A), null, HttpServletResponse.SC_NOT_FOUND, true) &&
                       getWithoutAssert(fullPathKey(KEY_B), null, HttpServletResponse.SC_NOT_FOUND, true);
            }
        }, 5000, 10);
    }
}
