package org.infinispan.server.test.client.memcached;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;

import org.infinispan.Version;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.test.util.ITestUtils.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.infinispan.server.test.util.ITestUtils.eventually;
import static org.infinispan.server.test.util.ITestUtils.sleepForSecs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
public abstract class AbstractSingleNodeMemcachedIT {

    static final String ENCODING = "UTF-8";

    // they are deleted in the setUp and tearDown methods
    static final String KEY_A = "a";
    static final String KEY_B = "b";
    static final String KEY_C = "c";

    private static final Log log = LogFactory.getLog(AbstractSingleNodeMemcachedIT.class);

    MemcachedClient mc;

    protected abstract RemoteInfinispanServer getServer();

    public static class TestSerializable implements Serializable {
        private String content;

        public TestSerializable(String content) {
            super();
            this.content = content;
        }

        public String getContent() {
            return content;
        }
    }

    @Before
    public void setUp() throws Exception {
        mc = new MemcachedClient(MemcachedSingleNodeIT.ENCODING, getServer().getMemcachedEndpoint().getInetAddress()
                .getHostName(), getServer().getMemcachedEndpoint().getPort(), 10000);
        mc.delete(KEY_A);
        mc.delete(KEY_B);
        mc.delete(KEY_C);
        assertNull(mc.get(KEY_A));
        assertNull(mc.get(KEY_B));
        assertNull(mc.get(KEY_C));
    }

    @After
    public void tearDown() throws Exception {
        mc.delete(KEY_A);
        mc.delete(KEY_B);
        mc.delete(KEY_C);
        mc.close();
    }

    @Test
    public void testSetGet() throws Exception {
        mc.set(KEY_A, "A");
        assertEquals("A", mc.get(KEY_A));
    }

    @Test
    public void testSetGetNewLineChars() throws Exception {
        mc.set(KEY_A, "A\r\nA");
        assertEquals("A\r\nA", mc.get(KEY_A));
    }

    @Test
    public void testSetGetObject() throws Exception {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(bout);
        oo.writeObject(new TestSerializable("CONTENT"));
        oo.flush();

        byte[] byteData = bout.toByteArray();
        mc.writeln("set " + KEY_A + " 0 0 " + byteData.length);
        mc.flush();
        mc.write(byteData);
        mc.write("\r\n".getBytes(ENCODING));
        mc.flush();
        assertEquals("STORED", mc.readln());
        byte[] bytesBack = mc.getBytes(KEY_A);
        assertEquals(byteData.length, bytesBack.length);
        ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(bytesBack));
        TestSerializable ts = (TestSerializable) oin.readObject();
        assertEquals("CONTENT", ts.getContent());
    }

    @Test
    public void testSetGetFlags() throws Exception {
        mc.writeln("set " + KEY_A + " 1234 0 1");
        mc.writeln("A");
        mc.flush();
        assertEquals("STORED", mc.readln());
        mc.writeln("get " + KEY_A);
        mc.flush();
        assertEquals("VALUE " + KEY_A + " 1234 1", mc.readln());
        assertEquals("A", mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testSecondSet() throws Exception {
        mc.set(KEY_A, "A");
        assertEquals("A", mc.get(KEY_A));
        mc.set(KEY_A, "B");
        assertEquals("B", mc.get(KEY_A));
    }

    @Test
    public void testGetMultipleKeys() throws Exception {
        mc.set(KEY_A, "A");
        mc.set(KEY_B, "B");
        mc.writeln("get " + KEY_A + " " + KEY_B);
        mc.flush();
        assertEquals("VALUE " + KEY_A + " 0 1", mc.readln());
        assertEquals("A", mc.readln());
        assertEquals("VALUE " + KEY_B + " 0 1", mc.readln());
        assertEquals("B", mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testAdd() throws Exception {
        String data = "testAdd";
        mc.writeln("add " + KEY_A + " 0 0 " + data.getBytes(ENCODING).length);
        mc.writeln(data);
        mc.flush();
        assertEquals("STORED", mc.readln());
        assertEquals(data, mc.get(KEY_A));
        // second add
        mc.writeln("add " + KEY_A + " 0 0 " + data.getBytes(ENCODING).length);
        mc.writeln(data);
        mc.flush();
        assertEquals("NOT_STORED", mc.readln());
    }

    @Test
    public void testReplace() throws Exception {
        mc.set(KEY_A, "testAdd");
        assertEquals("testAdd", mc.get(KEY_A));
        // replace
        mc.writeln("replace " + KEY_A + " 0 0 " + "replacement".getBytes(ENCODING).length);
        mc.writeln("replacement");
        mc.flush();
        assertEquals("STORED", mc.readln());
        assertEquals("replacement", mc.get(KEY_A));
    }

    @Test
    public void testReplaceNonExistent() throws Exception {
        mc.writeln("replace " + KEY_A + " 0 0 " + "replacement".getBytes(ENCODING).length);
        mc.writeln("replacement");
        mc.flush();
        assertEquals("NOT_STORED", mc.readln());
    }

    @Test
    public void testAppend() throws Exception {
        mc.set(KEY_A, "Hello");
        assertEquals("Hello", mc.get(KEY_A));
        mc.writeln("append " + KEY_A + " 0 0 " + ", World!".getBytes(ENCODING).length);
        mc.writeln(", World!");
        mc.flush();
        assertEquals("STORED", mc.readln());
        assertEquals("Hello, World!", mc.get(KEY_A));
    }

    @Test
    public void testAppendNonExistent() throws Exception {
        mc.writeln("append " + KEY_A + " 0 0 " + ", World!".getBytes(ENCODING).length);
        mc.writeln(", World!");
        mc.flush();
        assertEquals("NOT_STORED", mc.readln());
    }

    @Test
    public void testPrepend() throws Exception {
        mc.set(KEY_A, "World!");
        assertEquals("World!", mc.get(KEY_A));
        mc.writeln("prepend " + KEY_A + " 0 0 " + "Hello, ".getBytes(ENCODING).length);
        mc.writeln("Hello, ");
        mc.flush();
        assertEquals("STORED", mc.readln());
        assertEquals("Hello, World!", mc.get(KEY_A));
    }

    @Test
    public void testPrependNonExistent() throws Exception {
        mc.writeln("prepend " + KEY_A + " 0 0 " + "Hello, ".getBytes(ENCODING).length);
        mc.writeln("Hello, ");
        mc.flush();
        assertEquals("NOT_STORED", mc.readln());
    }

    @Test
    public void testCas() throws Exception {
        mc.set(KEY_A, "A");
        mc.writeln("gets " + KEY_A);
        mc.flush();
        String[] valueline = mc.readln().split(" ");
        assertEquals("VALUE", valueline[0]);
        assertEquals(KEY_A, valueline[1]);
        assertEquals("0", valueline[2]); // flags
        assertEquals("1", valueline[3]); // number of bytes in str "A"
        assertEquals("A", mc.readln());
        String casId = valueline[4];
        assertEquals("END", mc.readln());
        mc.writeln("cas " + KEY_A + " 0 0 1 " + casId);
        mc.writeln("B");
        mc.flush();
        assertEquals("STORED", mc.readln());
        assertEquals("B", mc.get(KEY_A));
        mc.writeln("gets " + KEY_A);
        mc.flush();
        valueline = mc.readln().split(" ");
        assertFalse(casId.equals(valueline[4]));
        assertEquals("B", mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testCasExists() throws Exception {
        mc.set(KEY_A, "A");
        mc.writeln("gets " + KEY_A);
        mc.flush();
        String[] valueline = mc.readln().split(" ");
        assertEquals("VALUE", valueline[0]);
        assertEquals(KEY_A, valueline[1]);
        assertEquals("0", valueline[2]); // flags
        assertEquals("1", valueline[3]); // number of bytes in str "A"
        assertEquals("A", mc.readln());
        String casId = valueline[4];
        assertEquals("END", mc.readln());
        mc.writeln("cas " + KEY_A + " 0 0 1 1" + casId); // note appended 1 before casId
        mc.writeln("B");
        mc.flush();
        assertEquals("EXISTS", mc.readln());
        assertEquals("A", mc.get(KEY_A));
    }

    @Test
    public void testGetNotFound() throws Exception {
        mc.writeln("get " + KEY_A);
        mc.flush();
        assertEquals("END", mc.readln());
    }

    @Test
    public void testCasNotFound() throws Exception {
        mc.writeln("cas " + KEY_A + " 0 0 1 1");
        mc.writeln("B");
        mc.flush();
        assertEquals("NOT_FOUND", mc.readln());
    }

    @Test
    public void testExpTime() throws Exception {
        mc.writeln("set " + KEY_A + " 0 2 1");
        mc.writeln("A");
        mc.flush();
        assertEquals("STORED", mc.readln());
        assertEquals("A", mc.get(KEY_A));
        sleepForSecs(2.5);
        assertNull(mc.get(KEY_A));
    }

    /**
     * NOTE: This is randomly failing test on various platforms.
     *
     * @throws Exception
     */
    @Test
    public void testExpTimeMaxSeconds() throws Exception {
        // set exp.time to max number treated as seconds
        // 2592000 = 60*60*24*30
        mc.writeln("set " + KEY_A + " 0 2592000 1");
        mc.writeln("A");
        mc.flush();
        assertEquals("STORED", mc.readln());
        assertEquals("A", mc.get(KEY_A));
        sleepForSecs(2);
        assertEquals("A", mc.get(KEY_A));
    }

    @Test
    public void testExpTimeAbsolutePast() throws Exception {
        // set exp.time to min number treated as unix time
        // 2592001 = 60*60*24*30 + 1
        // corresponding to Sat, 31 Jan 1970 00:00:01 GMT
        mc.writeln("set " + KEY_A + " 0 2592001 1");
        mc.writeln("A");
        mc.flush();
        assertEquals("STORED", mc.readln());
        // should expire immediately (the next millisecond)
        // the sleep here is only to make sure that System.currentTimeMillis()
        // increases before next get
        Thread.sleep(5);
        assertNull(mc.get(KEY_A));
    }

    @Test
    public void testExpTimeAbsoluteFuture() throws Exception {
        long now = mc.getServerTime();
        log.tracef("server time: " + now);
        mc.writeln("set " + KEY_A + " 0 " + (now + 2) + " 1");
        mc.writeln("A");
        mc.flush();
        assertEquals("STORED", mc.readln());
        assertEquals("A", mc.get(KEY_A));
        sleepForSecs(2);
        assertNull(mc.get(KEY_A));
    }

    private String key250() {
        String key250 = "";
        for (int i = 0; i < 250; i++) {
            key250 += "a";
        }
        return key250;
    }

    @Test
    public void testKeyLonger250() throws Exception {
        String key250 = key250();
        mc.delete(key250);
        String key251 = key250 + "a";
        // byte length is the same, because we're using "UTF-8 safe" char 'a'
        assertEquals(key250.length(), key250.getBytes(ENCODING).length);
        assertEquals(key251.length(), key251.getBytes(ENCODING).length);
        mc.set(key250, "A");
        assertEquals("A", mc.get(key250));
        mc.writeln("set " + key251 + " 0 0 1");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testKeyLonger250Get() throws Exception {
        mc.writeln("get " + key250() + "a");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testKeyLonger250Gets() throws Exception {
        mc.writeln("gets " + key250() + "a");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testKeyLonger250Multiget() throws Exception {
        String key250 = key250();
        String key251 = key250 + "a";
        // byte length is the same, because we're using "UTF-8 safe" char 'a'
        assertEquals(key250.length(), key250.getBytes(ENCODING).length);
        assertEquals(key251.length(), key251.getBytes(ENCODING).length);
        mc.set(key250, "A");
        assertEquals("A", mc.get(key250));
        mc.writeln("get " + key250 + " " + key251);
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
        assertEquals("DELETED", mc.delete(key250));
    }

    @Test
    public void testKeyLonger250Add() throws Exception {
        String key250 = key250();
        mc.delete(key250);
        String key251 = key250 + "a";
        // byte length is the same, because we're using "UTF-8 safe" char 'a'
        assertEquals(key250.length(), key250.getBytes(ENCODING).length);
        assertEquals(key251.length(), key251.getBytes(ENCODING).length);
        mc.writeln("add " + key250 + " 0 0 1");
        mc.writeln("A");
        mc.flush();
        assertEquals("STORED", mc.readln());
        assertEquals("A", mc.get(key250));
        mc.writeln("add " + key251 + " 0 0 1");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testKeyLonger250Replace() throws Exception {
        mc.writeln("replace " + key250() + "a 0 0 1");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testKeyLonger250Append() throws Exception {
        mc.writeln("append " + key250() + "a 0 0 1");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testKeyLonger250Prepend() throws Exception {
        mc.writeln("prepend " + key250() + "a 0 0 1");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testKeyLonger250Cas() throws Exception {
        mc.writeln("prepend " + key250() + "a 0 0 1 1");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testKeyLonger250Incr() throws Exception {
        mc.writeln("incr " + key250() + "a 1");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testKeyLonger250Decr() throws Exception {
        mc.writeln("decr " + key250() + "a 1");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testKeyLonger250Delete() throws Exception {
        mc.writeln("delete " + key250() + "a");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testDelete() throws Exception {
        mc.set(KEY_A, "A");
        assertEquals("A", mc.get(KEY_A));
        assertEquals("DELETED", mc.delete(KEY_A));
        assertNull(mc.get(KEY_A));
    }

    @Test
    public void testDeleteNonExistent() throws Exception {
        assertEquals("NOT_FOUND", mc.delete(KEY_A));
    }

    @Test
    public void testIncr() throws Exception {
        mc.set(KEY_A, "0");
        mc.writeln("incr " + KEY_A + " 1");
        mc.flush();
        assertEquals("1", mc.readln());
    }

    @Test
    public void testDecr() throws Exception {
        mc.set(KEY_A, "1");
        mc.writeln("decr " + KEY_A + " 1");
        mc.flush();
        assertEquals("0", mc.readln());
    }

    @Test
    public void testIncrNotFound() throws Exception {
        mc.writeln("incr " + KEY_A + " 1");
        mc.flush();
        assertEquals("NOT_FOUND", mc.readln());
    }

    @Test
    public void testDecrNotFound() throws Exception {
        mc.writeln("decr " + KEY_A + " 1");
        mc.flush();
        assertEquals("NOT_FOUND", mc.readln());
    }

    @Test
    public void testIncr64BitMax() throws Exception {
        mc.set(KEY_A, "18446744073709551614");
        mc.writeln("incr " + KEY_A + " 1");
        mc.flush();
        assertEquals("18446744073709551615", mc.readln());
        mc.writeln("incr " + KEY_A + " 1");
        mc.flush();
        assertEquals("0", mc.readln());
    }

    @Test
    public void testDecrZero() throws Exception {
        mc.set(KEY_A, "0");
        mc.writeln("decr " + KEY_A + " 1");
        mc.flush();
        assertEquals("0", mc.readln());
    }

    @Test
    public void testIncrBigIncrement() throws Exception {
        mc.set(KEY_A, "0");
        mc.writeln("incr " + KEY_A + " 18446744073709551615");
        mc.flush();
        assertEquals("18446744073709551615", mc.readln());
    }

    @Test
    public void testIncrBigDecrement() throws Exception {
        mc.set(KEY_A, "18446744073709551615");
        mc.writeln("decr " + KEY_A + " 18446744073709551615");
        mc.flush();
        assertEquals("0", mc.readln());
    }

    @Test
    public void testUnsupportedStats() throws Exception {
        Map<String, String> stats = mc.getStats();
        assertEquals(stats.get("pid"), "0");
        assertEquals(stats.get("pointer_size"), "0");
        assertEquals(stats.get("rusage_user"), "0");
        assertEquals(stats.get("rusage_system"), "0");
        assertEquals(stats.get("bytes"), "0");
        assertEquals(stats.get("curr_connections"), "0");
        assertEquals(stats.get("total_connections"), "0");
        assertEquals(stats.get("connection_structures"), "0");
        assertEquals(stats.get("auth_cmds"), "0");
        assertEquals(stats.get("auth_errors"), "0");
        assertEquals(stats.get("limit_maxbytes"), "0");
        assertEquals(stats.get("conn_yields"), "0");
        assertEquals(stats.get("threads"), "0");
    }

    @Test
    public void testStatsBytes() throws Exception {
        Map<String, String> stats = mc.getStats();
        int bytesRead = new Integer(stats.get("bytes_read"));
        int bytesWritten = new Integer(stats.get("bytes_written"));

        mc.set("testKey", "testValue");
        mc.get("testKey");
        mc.delete("testKey");

        stats = mc.getStats();
        assertTrue("Bytes read didnt increase.", bytesRead < Integer.parseInt(stats.get("bytes_read")));
        assertTrue("Bytes written didnt increase.", bytesWritten < Integer.parseInt(stats.get("bytes_written")));
    }

    @Test
    public void testStatsTime() throws Exception {
        Map<String, String> stats = mc.getStats();
        int uptime = new Integer(stats.get("uptime"));
        int time = new Integer(stats.get("time"));
        sleepForSecs(1);
        stats = mc.getStats();
        assertTrue(uptime < new Integer(stats.get("uptime")));
        assertTrue(time < new Integer(stats.get("time")));
    }

    @Test
    public void testStatsVersion() throws Exception {
        Map<String, String> stats = mc.getStats();
        String version = stats.get("version");
        assertNotNull(version);
        assertTrue(version.startsWith(Version.getMajorMinor()));
    }

    @Test
    public void testStatsGetSetItemCount() throws Exception {
        Map<String, String> stats = mc.getStats();
        int cmd_set = new Integer(stats.get("cmd_set"));
        int cmd_get = new Integer(stats.get("cmd_get"));
        int get_hits = new Integer(stats.get("get_hits"));
        int get_misses = new Integer(stats.get("get_misses"));
        int curr_items = new Integer(stats.get("curr_items"));
        int total_items = new Integer(stats.get("total_items"));
        mc.set(KEY_A, "A");
        assertEquals("A", mc.get(KEY_A));
        assertNull(mc.get(KEY_B));
        stats = mc.getStats();
        int cmd_set_new = new Integer(stats.get("cmd_set"));
        int cmd_get_new = new Integer(stats.get("cmd_get"));
        int get_hits_new = new Integer(stats.get("get_hits"));
        int get_misses_new = new Integer(stats.get("get_misses"));
        int curr_items_new = new Integer(stats.get("curr_items"));
        int total_items_new = new Integer(stats.get("total_items"));
        assertEquals(cmd_get + 2, cmd_get_new);
        assertEquals(cmd_set + 1, cmd_set_new);
        assertEquals(get_hits + 1, get_hits_new);
        assertEquals(get_misses + 1, get_misses_new);
        assertEquals(curr_items + 1, curr_items_new);
        assertEquals(total_items + 1, total_items_new);
    }

    @Test
    public void testStatsDelete() throws Exception {
        Map<String, String> stats = mc.getStats();
        int delete_misses = new Integer(stats.get("delete_misses"));
        int delete_hits = new Integer(stats.get("delete_hits"));
        mc.set(KEY_A, "A");
        assertEquals("DELETED", mc.delete(KEY_A));
        assertEquals("NOT_FOUND", mc.delete(KEY_B));
        stats = mc.getStats();
        int delete_misses_new = new Integer(stats.get("delete_misses"));
        int delete_hits_new = new Integer(stats.get("delete_hits"));
        assertEquals(delete_misses + 1, delete_misses_new);
        assertEquals(delete_hits + 1, delete_hits_new);
    }

    @Test
    public void testStatsIncrDecr() throws Exception {
        Map<String, String> stats = mc.getStats();
        int incr_misses = new Integer(stats.get("incr_misses"));
        int incr_hits = new Integer(stats.get("incr_hits"));
        int decr_misses = new Integer(stats.get("decr_misses"));
        int decr_hits = new Integer(stats.get("decr_hits"));

        mc.set(KEY_A, "0");
        mc.writeln("incr " + KEY_A + " 1");
        mc.flush();
        assertEquals("1", mc.readln());
        mc.writeln("decr " + KEY_A + " 1");
        mc.flush();
        assertEquals("0", mc.readln());
        mc.writeln("incr " + KEY_B + " 1");
        mc.flush();
        assertEquals("NOT_FOUND", mc.readln());
        mc.writeln("decr " + KEY_B + " 1");
        mc.flush();
        assertEquals("NOT_FOUND", mc.readln());

        stats = mc.getStats();
        int incr_misses_new = new Integer(stats.get("incr_misses"));
        int incr_hits_new = new Integer(stats.get("incr_hits"));
        int decr_misses_new = new Integer(stats.get("decr_misses"));
        int decr_hits_new = new Integer(stats.get("decr_hits"));
        assertEquals(incr_misses + 1, incr_misses_new);
        assertEquals(incr_hits + 1, incr_hits_new);
        assertEquals(decr_misses + 1, decr_misses_new);
        assertEquals(decr_hits + 1, decr_hits_new);
    }

    @Test
    public void testStatsCas() throws Exception {
        Map<String, String> stats = mc.getStats();
        int cas_misses = new Integer(stats.get("cas_misses"));
        int cas_hits = new Integer(stats.get("cas_hits"));

        // cas hit
        mc.set(KEY_A, "A");
        mc.writeln("gets " + KEY_A);
        mc.flush();
        String[] valueline = mc.readln().split(" ");
        assertEquals("VALUE", valueline[0]);
        assertEquals(KEY_A, valueline[1]);
        assertEquals("0", valueline[2]); // flags
        assertEquals("1", valueline[3]); // number of bytes in str "A"
        assertEquals("A", mc.readln());
        String casId = valueline[4];
        assertEquals("END", mc.readln());
        mc.writeln("cas " + KEY_A + " 0 0 1 " + casId);
        mc.writeln("B");
        mc.flush();
        assertEquals("STORED", mc.readln());
        assertEquals("B", mc.get(KEY_A));

        // cas miss
        mc.writeln("cas " + KEY_B + " 0 0 1 1");
        mc.writeln("B");
        mc.flush();
        assertEquals("NOT_FOUND", mc.readln());

        stats = mc.getStats();
        int cas_misses_new = new Integer(stats.get("cas_misses"));
        int cas_hits_new = new Integer(stats.get("cas_hits"));
        assertEquals(cas_misses + 1, cas_misses_new);
        assertEquals(cas_hits + 1, cas_hits_new);

        int cas_badval = new Integer(stats.get("cas_badval"));

        // cas bad value
        mc.writeln("cas " + KEY_A + " 0 0 1 1" + casId); // note appended 1 before casId
        mc.writeln("C");
        mc.flush();
        assertEquals("EXISTS", mc.readln());

        stats = mc.getStats();
        int cas_badval_new = new Integer(stats.get("cas_badval"));
        assertEquals(cas_badval + 1, cas_badval_new);
    }

    @Test
    public void testBogusCommand() throws Exception {
        mc.writeln("boguscommand");
        mc.flush();
        assertStartsWith(mc.readln(), "ERROR");
    }

    @Test
    public void testBogusCommandArgs() throws Exception {
        mc.writeln("boguscommand arg1 arg2 arg3");
        mc.flush();
        assertStartsWith(mc.readln(), "ERROR");
    }

    @Test
    public void testBogusCommandPipeline() throws Exception {
        mc.writeln("boguscommand");
        mc.writeln("delete " + KEY_A);
        mc.flush();
        assertStartsWith(mc.readln(), "ERROR");
        assertEquals("NOT_FOUND", mc.readln());
    }

    @Test
    public void testCasParsing1() throws Exception {
        mc.writeln("cas bad blah 0 0 0");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testCasParsing2() throws Exception {
        mc.writeln("cas bad 0 blah 0 0");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testCasParsing3() throws Exception {
        mc.writeln("cas bad 0 0 blah 0");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testCasParsing4() throws Exception {
        mc.writeln("cas bad 0 0 0 blah");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    // missing cas unique value
    @Test
    public void testCasParsing6() throws Exception {
        mc.writeln("cas bad 0 0 6");
        mc.flush();
        assertStartsWith(mc.readln(), "SERVER_ERROR");
    }

    @Test
    public void testCasUniqueIs64Bit() throws Exception {
        // casid should be 64 bit value, in our case signed
        // ie. should have range Long.MIN_VALUE .. Long.MAX_VALUE
        mc.writeln("cas a 0 0 1 " + Long.MAX_VALUE);
        mc.writeln("a");
        mc.flush();
        assertEquals("NOT_FOUND", mc.readln());
        mc.writeln("cas a 0 0 1 " + Long.MIN_VALUE);
        mc.writeln("a");
        mc.flush();
        assertEquals("NOT_FOUND", mc.readln());
    }

    @Test
    public void testSetFlagsRange() throws Exception {
        mc.writeln("set a 0 0 1");
        mc.writeln("a");
        mc.flush();
        assertEquals("STORED", mc.readln());
        mc.writeln("set a 4294967295 0 1");
        mc.writeln("a");
        mc.flush();
        assertEquals("STORED", mc.readln());
        mc.writeln("set a -1 0 1");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
        mc.writeln("set a 4294967296 0 1");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testVerbosityUnsupported() throws Exception {
        mc.writeln("verbosity 0");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testQuit() throws Exception {
        mc.writeln("quit");
        mc.flush();
        assertEquals("", mc.readln());
    }

    @Test
    public void testFlushAll() throws Exception {
        mc.set(KEY_A, "valA");
        mc.set(KEY_B, "valB");
        mc.set(KEY_C, "valC");
        assertEquals("valA", mc.get(KEY_A));
        assertEquals("valB", mc.get(KEY_B));
        assertEquals("valC", mc.get(KEY_C));
        mc.writeln("flush_all");
        mc.flush();
        assertEquals("OK", mc.readln());
        assertNull(mc.get(KEY_A));
        assertNull(mc.get(KEY_B));
        assertNull(mc.get(KEY_C));
    }

    @Test
    public void testFlushAllDelayed() throws Exception {
        mc.set(KEY_A, "valA");
        mc.set(KEY_B, "valB");
        mc.set(KEY_C, "valC");
        assertEquals("valA", mc.get(KEY_A));
        assertEquals("valB", mc.get(KEY_B));
        assertEquals("valC", mc.get(KEY_C));
        mc.writeln("flush_all 1");
        mc.flush();
        assertEquals("OK", mc.readln());
        assertEquals("valA", mc.get(KEY_A));
        assertEquals("valB", mc.get(KEY_B));
        assertEquals("valC", mc.get(KEY_C));
        eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return mc.get(KEY_A) == null && mc.get(KEY_B) == null && mc.get(KEY_C) == null;
            }
        }, 20000, 40);
    }

    @Test
    public void testFlushAllDelayedUnixTime() throws Exception {
        mc.set(KEY_A, "valA");
        mc.set(KEY_B, "valB");
        mc.set(KEY_C, "valC");
        assertEquals("valA", mc.get(KEY_A));
        assertEquals("valB", mc.get(KEY_B));
        assertEquals("valC", mc.get(KEY_C));
        long t = mc.getServerTime();
        assertTrue(t > 0);
        mc.writeln("flush_all " + (t + 2));
        mc.flush();
        assertEquals("OK", mc.readln());
        assertEquals("valA", mc.get(KEY_A));
        assertEquals("valB", mc.get(KEY_B));
        assertEquals("valC", mc.get(KEY_C));
        eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return mc.get(KEY_A) == null && mc.get(KEY_B) == null && mc.get(KEY_C) == null;
            }
        }, 20000, 40);
    }

    @Test
    public void testPipeliningSet() throws Exception {
        mc.writeln("set " + KEY_A + " 0 0 1");
        mc.writeln("a");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("STORED", mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testPipeliningAdd() throws Exception {
        mc.writeln("add " + KEY_A + " 0 0 1");
        mc.writeln("a");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("STORED", mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testPipeliningReplace() throws Exception {
        mc.set(KEY_A, "b");
        mc.writeln("replace " + KEY_A + " 0 0 1");
        mc.writeln("a");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("STORED", mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testPipeliningAppend() throws Exception {
        mc.set(KEY_A, "a");
        mc.writeln("append " + KEY_A + " 0 0 1");
        mc.writeln("a");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("STORED", mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testPipeliningPrepend() throws Exception {
        mc.set(KEY_A, "a");
        mc.writeln("prepend " + KEY_A + " 0 0 1");
        mc.writeln("a");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("STORED", mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testPipeliningCas() throws Exception {
        mc.writeln("cas " + KEY_A + " 0 0 1 0");
        mc.writeln("a");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("NOT_FOUND", mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testPipeliningGet() throws Exception {
        mc.writeln("get " + KEY_A);
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("END", mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testPipeliningGetMulti() throws Exception {
        mc.writeln("get " + KEY_A + " " + KEY_C);
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("END", mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testPipeliningGets() throws Exception {
        mc.writeln("gets " + KEY_A);
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("END", mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testPipeliningGetsMulti() throws Exception {
        mc.writeln("gets " + KEY_A + " " + KEY_C);
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("END", mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testPipeliningDelete() throws Exception {
        mc.writeln("delete " + KEY_A);
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("NOT_FOUND", mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testPipeliningIncr() throws Exception {
        mc.set(KEY_A, "1");
        mc.writeln("incr " + KEY_A + " 1");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("2", mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testPipeliningDecr() throws Exception {
        mc.set(KEY_A, "2");
        mc.writeln("decr " + KEY_A + " 1");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("1", mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testPipeliningStats() throws Exception {
        mc.writeln("stats");
        mc.writeln("get " + KEY_B);
        mc.flush();
        String line = null;
        do {
            line = mc.readln();
        } while (!line.equals("END"));
        assertEquals("END", mc.readln());
    }

    @Test
    public void testPipeliningVersion() throws Exception {
        mc.writeln("version");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertNotNull(mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testPipeliningVerbosity() throws Exception {
        mc.writeln("verbosity 0");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
        assertEquals("END", mc.readln());
    }

    @Test
    public void testPipeliningFlushAll() throws Exception {
        mc.writeln("flush_all");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("OK", mc.readln());
        assertEquals("END", mc.readln());
    }

    @Test
    public void testPipeliningFlushAllDelayed() throws Exception {
        mc.writeln("flush_all 1");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("OK", mc.readln());
        assertEquals("END", mc.readln());
        mc.set(KEY_A, "thisWillBeFlushed");
        assertEquals("thisWillBeFlushed", mc.get(KEY_A));
        eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return mc.get(KEY_A) == null;
            }
        }, 20000, 40);
    }

    @Test
    public void testStatsArgs() throws Exception {
        mc.writeln("stats args");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testNegativeItemSize() throws Exception {
        mc.writeln("set a 0 0 -1");
        mc.flush();
        assertStartsWith(mc.readln(), "CLIENT_ERROR");
    }

    @Test
    public void testNoReplySet() throws Exception {
        mc.writeln("set " + KEY_A + " 0 0 1 noreply");
        mc.writeln("a");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("END", mc.readln());
    }

    @Test
    public void testNoReplyAdd() throws Exception {
        mc.writeln("add " + KEY_A + " 0 0 1 noreply");
        mc.writeln("a");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("END", mc.readln());
    }

    @Test
    public void testNoReplyReplace() throws Exception {
        mc.set(KEY_A, "b");
        mc.writeln("replace " + KEY_A + " 0 0 1 noreply");
        mc.writeln("a");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("END", mc.readln());
    }

    @Test
    public void testNoReplyAppend() throws Exception {
        mc.set(KEY_A, "a");
        mc.writeln("append " + KEY_A + " 0 0 1 noreply");
        mc.writeln("a");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("END", mc.readln());
    }

    @Test
    public void testNoReplyPrepend() throws Exception {
        mc.set(KEY_A, "a");
        mc.writeln("prepend " + KEY_A + " 0 0 1 noreply");
        mc.writeln("a");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("END", mc.readln());
    }

    @Test
    public void testNoReplyCas() throws Exception {
        mc.writeln("cas " + KEY_A + " 0 0 1 0 noreply");
        mc.writeln("a");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("END", mc.readln());
    }

    @Test
    public void testNoReplyDelete() throws Exception {
        mc.writeln("delete " + KEY_A + " noreply");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("END", mc.readln());
    }

    @Test
    public void testNoReplyIncr() throws Exception {
        mc.set(KEY_A, "1");
        mc.writeln("incr " + KEY_A + " 1 noreply");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("END", mc.readln());
    }

    @Test
    public void testNoReplyDecr() throws Exception {
        mc.set(KEY_A, "2");
        mc.writeln("decr " + KEY_A + " 1 noreply");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("END", mc.readln());
    }

    @Test
    public void testNoReplyFlushAll() throws Exception {
        mc.writeln("flush_all noreply");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("END", mc.readln());
    }

    @Test
    public void testNoReplyFlushAllDelayed() throws Exception {
        mc.writeln("flush_all 1 noreply");
        mc.writeln("get " + KEY_B);
        mc.flush();
        assertEquals("END", mc.readln());
        mc.set(KEY_A, "thisWillBeFlushed");
        assertEquals("thisWillBeFlushed", mc.get(KEY_A));
        eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
                return mc.get(KEY_A) == null;
            }
        }, 20000, 40);
    }

    private void assertStartsWith(String str, String prefix) {
        assertTrue("String \"" + str + "\" doesn't start with expected prefix \"" + prefix + "\"", str.startsWith(prefix));
    }

}
