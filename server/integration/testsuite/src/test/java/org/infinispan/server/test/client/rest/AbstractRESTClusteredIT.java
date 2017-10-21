package org.infinispan.server.test.client.rest;

import static org.infinispan.server.test.client.rest.RESTHelper.KEY_A;
import static org.infinispan.server.test.client.rest.RESTHelper.KEY_B;
import static org.infinispan.server.test.client.rest.RESTHelper.KEY_C;
import static org.infinispan.server.test.util.ITestUtils.isReplicatedMode;
import static org.infinispan.server.test.util.ITestUtils.sleepForSecs;

import java.util.List;

import org.apache.http.HttpStatus;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the RESTLocal client.
 *
 * @author <a href="mailto:jvilkola@redhat.com">Jozef Vilkolak</a>
 * @author <a href="mailto:mlinhard@redhat.com">Michal Linhard</a>
 * @author mgencur
 */
public abstract class AbstractRESTClusteredIT {

    protected abstract int getRestPort1();
    protected abstract int getRestPort2();

    protected abstract List<RemoteInfinispanServer> getServers();
    protected RESTHelper rest;

    @Before
    public void setUp() throws Exception {
        rest = new RESTHelper();
        if (isReplicatedMode()) {
            rest.addServer(getServers().get(0).getRESTEndpoint().getInetAddress().getHostName(), getRestPort1(), getServers().get(0).getRESTEndpoint().getContextPath());
            rest.addServer(getServers().get(1).getRESTEndpoint().getInetAddress().getHostName(), getRestPort2(), getServers().get(1).getRESTEndpoint().getContextPath());
        } else {
            rest.addServer(getServers().get(0).getRESTEndpoint().getInetAddress().getHostName(), getServers().get(0).getRESTEndpoint().getContextPath());
            rest.addServer(getServers().get(1).getRESTEndpoint().getInetAddress().getHostName(), getServers().get(1).getRESTEndpoint().getContextPath());
        }

        rest.delete(rest.fullPathKey(KEY_A));
        rest.delete(rest.fullPathKey(KEY_B));
        rest.delete(rest.fullPathKey(KEY_C));

        rest.head(rest.fullPathKey(KEY_A), HttpStatus.SC_NOT_FOUND);
        rest.head(rest.fullPathKey(KEY_B), HttpStatus.SC_NOT_FOUND);
        rest.head(rest.fullPathKey(KEY_C), HttpStatus.SC_NOT_FOUND);
    }

    @After
    public void tearDown() throws Exception {
        rest.delete(rest.fullPathKey(KEY_A));
        rest.delete(rest.fullPathKey(KEY_B));
        rest.delete(rest.fullPathKey(KEY_C));
        rest.clearServers();
    }

    @Test
    public void testReplicationPut() throws Exception {
        rest.put(rest.fullPathKey(0, KEY_A), "data", "text/plain");
        rest.get(rest.fullPathKey(1, KEY_A), "data");
    }

    @Test
    public void testReplicationPost() throws Exception {
        rest.post(rest.fullPathKey(0, KEY_A), "data", "text/plain");
        rest.get(rest.fullPathKey(1, KEY_A), "data");
    }

    @Test
    public void testReplicationDelete() throws Exception {
        rest.post(rest.fullPathKey(0, KEY_A), "data", "text/plain");
        rest.get(rest.fullPathKey(1, KEY_A), "data");
        rest.delete(rest.fullPathKey(0, KEY_A));
        rest.head(rest.fullPathKey(1, KEY_A), HttpStatus.SC_NOT_FOUND);
    }

    @Test
    public void testReplicationWipeCache() throws Exception {
        rest.post(rest.fullPathKey(0, KEY_A), "data", "text/plain");
        rest.post(rest.fullPathKey(0, KEY_B), "data", "text/plain");
        rest.head(rest.fullPathKey(0, KEY_A));
        rest.head(rest.fullPathKey(0, KEY_B));
        rest.delete(rest.fullPathKey(0, null));
        rest.head(rest.fullPathKey(1, KEY_A), HttpStatus.SC_NOT_FOUND);
        rest.head(rest.fullPathKey(1, KEY_B), HttpStatus.SC_NOT_FOUND);
    }

    @Test
    public void testReplicationTTL() throws Exception {
        rest.post(rest.fullPathKey(0, KEY_A), "data", "text/plain", HttpStatus.SC_OK,
                // headers
                "Content-Type", "text/plain", "timeToLiveSeconds", "2");
        rest.head(rest.fullPathKey(1, KEY_A));
        sleepForSecs(2.1);
        // should be evicted
        rest.head(rest.fullPathKey(1, KEY_A), HttpStatus.SC_NOT_FOUND);
    }
}
