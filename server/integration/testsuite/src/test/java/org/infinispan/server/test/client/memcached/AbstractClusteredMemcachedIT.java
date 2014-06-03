package org.infinispan.server.test.client.memcached;

import java.util.List;

import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for Memcached endpoint. Subclasses must provide a way to get the list of remote
 * infinispan servers.
 * <p/>
 * Subclasses may be used in Client-Server mode or Hybrid mode where Memcached server
 * runs as a library deployed in an application server.
 *
 * @author Michal Linhard
 * @author Martin Gencur
 * @author Jozef Vilkolak
 */
public abstract class AbstractClusteredMemcachedIT {

    static final String ENCODING = "UTF-8";
    // keys used during tests
    // they are deleted in the setUp and tearDown methods
    static final String KEY_A = "a";
    static final String KEY_B = "b";
    static final String KEY_C = "c";

    MemcachedClient mc1;
    MemcachedClient mc2;

    protected abstract List<RemoteInfinispanServer> getServers();

    @Before
    public void setUp() throws Exception {
        mc1 = new MemcachedClient(ENCODING, getServers().get(0).getMemcachedEndpoint().getInetAddress()
                .getHostName(), getServers().get(0).getMemcachedEndpoint().getPort(), 10000); // to run against
        // infinispan
        // memcached
        // server
        mc2 = new MemcachedClient(ENCODING, getServers().get(1).getMemcachedEndpoint().getInetAddress()
                .getHostName(), getServers().get(1).getMemcachedEndpoint().getPort(), 10000); // to run against
        // infinispan
        // memcached
        // server
        mc1.delete(KEY_A);
        mc1.delete(KEY_B);
        mc1.delete(KEY_C);
        mc2.delete(KEY_A);
        mc2.delete(KEY_B);
        mc2.delete(KEY_C);
        assertNull(mc1.get(KEY_A));
        assertNull(mc1.get(KEY_B));
        assertNull(mc1.get(KEY_C));
        assertNull(mc2.get(KEY_A));
        assertNull(mc2.get(KEY_B));
        assertNull(mc2.get(KEY_C));
    }

    @After
    public void tearDown() throws Exception {
        mc1.delete(KEY_A);
        mc1.delete(KEY_B);
        mc1.delete(KEY_C);
        mc2.delete(KEY_A);
        mc2.delete(KEY_B);
        mc2.delete(KEY_C);
        mc1.close();
        mc2.close();
    }

    @Test
    public void testReplicatedSet() throws Exception {
        mc1.set(KEY_A, "A");
        assertEquals("A", mc2.get(KEY_A));
    }

    @Test
    public void testReplicatedGetMultipleKeys() throws Exception {
        mc1.set(KEY_A, "A");
        mc1.set(KEY_B, "B");
        mc1.set(KEY_C, "C");
        mc2.writeln("get " + KEY_A + " " + KEY_B + " " + KEY_C);
        mc2.flush();
        assertEquals("VALUE " + KEY_A + " 0 1", mc2.readln());
        assertEquals("A", mc2.readln());
        assertEquals("VALUE " + KEY_B + " 0 1", mc2.readln());
        assertEquals("B", mc2.readln());
        assertEquals("VALUE " + KEY_C + " 0 1", mc2.readln());
        assertEquals("C", mc2.readln());
        assertEquals("END", mc2.readln());
    }

    @Test
    public void testReplicatedAdd() throws Exception {
        String data = "testAdd";
        mc1.writeln("add " + KEY_A + " 0 0 " + data.getBytes(ENCODING).length);
        mc1.writeln(data);
        mc1.flush();
        assertEquals("STORED", mc1.readln());
        assertEquals(data, mc2.get(KEY_A));
    }

    @Test
    public void testReplicatedReplace() throws Exception {
        mc1.set(KEY_A, "data1");
        assertEquals("data1", mc2.get(KEY_A));
        mc1.writeln("replace " + KEY_A + " 0 0 " + "data2".getBytes(ENCODING).length);
        mc1.writeln("data2");
        mc1.flush();
        assertEquals("STORED", mc1.readln());
        assertEquals("data2", mc2.get(KEY_A));
    }

    @Test
    public void testReplicatedAppend() throws Exception {
        mc1.set(KEY_A, "Hello");
        assertEquals("Hello", mc2.get(KEY_A));
        mc2.writeln("append " + KEY_A + " 0 0 " + ", World!".getBytes(ENCODING).length);
        mc2.writeln(", World!");
        mc2.flush();
        assertEquals("STORED", mc2.readln());
        assertEquals("Hello, World!", mc1.get(KEY_A));
    }

    @Test
    public void testReplicatedPrepend() throws Exception {
        mc1.set(KEY_A, "World!");
        assertEquals("World!", mc1.get(KEY_A));
        mc2.writeln("prepend " + KEY_A + " 0 0 " + "Hello, ".getBytes(ENCODING).length);
        mc2.writeln("Hello, ");
        mc2.flush();
        assertEquals("STORED", mc2.readln());
        assertEquals("Hello, World!", mc1.get(KEY_A));
    }

    @Test
    public void testReplicatedCas() throws Exception {
        mc1.set(KEY_A, "A");
        String casId = mc2.getCasId(KEY_A);
        mc2.writeln("cas " + KEY_A + " 0 0 1 " + casId);
        mc2.writeln("B");
        mc2.flush();
        assertEquals("STORED", mc2.readln());
        assertEquals("B", mc1.get(KEY_A));
    }

    @Test
    public void testReplicatedCasExists() throws Exception {
        mc1.set(KEY_A, "A");
        String casId = mc2.getCasId(KEY_A);
        mc2.writeln("cas " + KEY_A + " 0 0 1 1" + casId); // note appended 1 before casId
        mc2.writeln("B");
        mc2.flush();
        assertEquals("EXISTS", mc2.readln());
        assertEquals("A", mc1.get(KEY_A));
    }

    @Test
    public void testReplicatedCasExists2() throws Exception {
        mc1.set(KEY_A, "A");
        String casId1 = mc1.getCasId(KEY_A);
        String casId2 = mc2.getCasId(KEY_A);
        assertEquals(casId1, casId2);

        mc2.writeln("cas " + KEY_A + " 0 0 2 " + casId1);
        mc2.writeln("B2");
        mc2.flush();
        assertEquals("STORED", mc2.readln());

        mc1.writeln("cas " + KEY_A + " 0 0 2 " + casId1);
        mc1.writeln("B1");
        mc1.flush();
        assertEquals("EXISTS", mc1.readln());
    }

    @Test
    public void testReplicatedDelete() throws Exception {
        mc1.set(KEY_A, "A");
        assertEquals("A", mc2.get(KEY_A));
        assertEquals("DELETED", mc1.delete(KEY_A));
        assertNull(mc2.get(KEY_A));
    }

    @Test
    public void testReplicatedIncrement() throws Exception {
        mc1.set(KEY_A, "0");
        mc2.writeln("incr " + KEY_A + " 1");
        mc2.flush();
        assertEquals("1", mc2.readln());
    }

    @Test
    public void testReplicatedDecrement() throws Exception {
        mc1.set(KEY_A, "1");
        mc2.writeln("decr " + KEY_A + " 1");
        mc2.flush();
        assertEquals("0", mc2.readln());
    }

    @Test
    public void testReplicatedFlushAll() throws Exception {
        // flush_all command is not replicated across cluster
        // it runs locally
        // -- but galderz changed it https://github.com/infinispan/infinispan/commit/585f28cab5df8e6806ad92dcc40d61b82a09ff86
        mc1.set(KEY_A, "1");
        mc1.set(KEY_B, "2");
        mc1.set(KEY_C, "3");
        mc2.writeln("flush_all");
        mc2.flush();
        assertEquals("OK", mc2.readln());
        assertNull(mc1.get(KEY_A));
        assertNull(mc1.get(KEY_B));
        assertNull(mc1.get(KEY_C));
    }

}
