package org.infinispan.server.memcached;

import static org.infinispan.server.memcached.ConnectionStatsTest.testMultipleLocalConnections;
import static org.infinispan.server.memcached.ConnectionStatsTest.testSingleLocalConnection;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.createMemcachedClient;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.sleepThread;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import org.infinispan.Version;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jgroups.util.Triple;
import org.testng.annotations.Test;

import net.spy.memcached.CASValue;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

/**
 * Tests stats command for Infinispan Memcached server.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.memcached.MemcachedStatsTest")
public class MemcachedStatsTest extends MemcachedSingleNodeTest {

   private static String jmxDomain = MemcachedStatsTest.class.getSimpleName();

   @Override
   public EmbeddedCacheManager createTestCacheManager() {
      return TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(jmxDomain);
   }

   public void testUnsupportedStats(Method m) {
      Triple<Map<String, String>, Integer, Integer> stats = getStats(-1, -1);
      assertEquals(stats.getVal1().get("pid"), "0");
      assertEquals(stats.getVal1().get("pointer_size"), "0");
      assertEquals(stats.getVal1().get("rusage_user"), "0");
      assertEquals(stats.getVal1().get("rusage_system"), "0");
      assertEquals(stats.getVal1().get("bytes"), "0");
      assertEquals(stats.getVal1().get("connection_structures"), "0");
      assertEquals(stats.getVal1().get("auth_cmds"), "0");
      assertEquals(stats.getVal1().get("auth_errors"), "0");
      assertEquals(stats.getVal1().get("limit_maxbytes"), "0");
      assertEquals(stats.getVal1().get("conn_yields"), "0");
      assertEquals(stats.getVal1().get("reclaimed"), "0");
   }

   public void testUncomparableStats(Method m) {
      sleepThread(TimeUnit.SECONDS.toMillis(1));
      Triple<Map<String, String>, Integer, Integer> stats = getStats(-1, -1);
      assertNotSame(stats.getVal1().get("uptime"), "0");
      assertNotSame(stats.getVal1().get("time"), "0");
      assertNotSame(stats.getVal1().get("uptime"), stats.getVal1().get("time"));
   }

   public void testStaticStats(Method m) {
       Triple<Map<String, String>, Integer, Integer> stats = getStats(-1, -1);
       assertEquals(stats.getVal1().get("version"), Version.getVersion());
    }

   public void testTodoStats() {
      Triple<Map<String, String>, Integer, Integer> stats = getStats(-1, -1);
      assertEquals(stats.getVal1().get("curr_connections"), "0");
      assertEquals(stats.getVal1().get("total_connections"), "0");
      assertEquals(stats.getVal1().get("threads"), "0");
   }

   public void testStats(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      Triple<Map<String, String>, Integer, Integer> stats = getStats(-1, -1);
      assertEquals(stats.getVal1().get("cmd_set"), "0");
      assertEquals(stats.getVal1().get("cmd_get"), "0");
      assertEquals(stats.getVal1().get("get_hits"), "0");
      assertEquals(stats.getVal1().get("get_misses"), "0");
      assertEquals(stats.getVal1().get("delete_hits"), "0");
      assertEquals(stats.getVal1().get("delete_misses"), "0");
      assertEquals(stats.getVal1().get("curr_items"), "0");
      assertEquals(stats.getVal1().get("total_items"), "0");
      assertEquals(stats.getVal1().get("incr_misses"), "0");
      assertEquals(stats.getVal1().get("incr_hits"), "0");
      assertEquals(stats.getVal1().get("decr_misses"), "0");
      assertEquals(stats.getVal1().get("decr_hits"), "0");
      assertEquals(stats.getVal1().get("cas_misses"), "0");
      assertEquals(stats.getVal1().get("cas_hits"), "0");
      assertEquals(stats.getVal1().get("cas_badval"), "0");

      OperationFuture<Boolean> f = client.set(k(m), 0, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(k(m)), v(m));
      f = client.set(k(m, "k1-"), 0, v(m, "v1-"));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(k(m, "k1-")), v(m, "v1-"));
      stats = getStats(stats.getVal2(), stats.getVal3());
      assertEquals(stats.getVal1().get("cmd_set"), "2");
      assertEquals(stats.getVal1().get("cmd_get"), "2");
      assertEquals(stats.getVal1().get("get_hits"), "2");
      assertEquals(stats.getVal1().get("get_misses"), "0");
      assertEquals(stats.getVal1().get("delete_hits"), "0");
      assertEquals(stats.getVal1().get("delete_misses"), "0");
      assertEquals(stats.getVal1().get("curr_items"), "2");
      assertEquals(stats.getVal1().get("total_items"), "2");

      f = client.delete(k(m, "k1-"));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      stats = getStats(stats.getVal2(), stats.getVal3());
      assertEquals(stats.getVal1().get("curr_items"), "1");
      assertEquals(stats.getVal1().get("total_items"), "2");
      assertEquals(stats.getVal1().get("delete_hits"), "1");
      assertEquals(stats.getVal1().get("delete_misses"), "0");

      assertNull(client.get(k(m, "k99-")));
      stats = getStats(stats.getVal2(), stats.getVal3());
      assertEquals(stats.getVal1().get("get_hits"), "2");
      assertEquals(stats.getVal1().get("get_misses"), "1");

      f = client.delete(k(m, "k99-"));
      assertFalse(f.get(timeout, TimeUnit.SECONDS));
      stats = getStats(stats.getVal2(), stats.getVal3());
      assertEquals(stats.getVal1().get("delete_hits"), "1");
      assertEquals(stats.getVal1().get("delete_misses"), "1");

      int future = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() + 1000);
      f = client.set(k(m, "k3-"), future, v(m, "v3-"));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      sleepThread(1100);
      assertNull(client.get(k(m, "k3-")));
      stats = getStats(stats.getVal2(), stats.getVal3());
      assertEquals(stats.getVal1().get("curr_items"), "1");
      assertEquals(stats.getVal1().get("total_items"), "3");

      client.incr(k(m, "k4-"), 1);
      stats = getStats(stats.getVal2(), stats.getVal3());
      assertEquals(stats.getVal1().get("incr_misses"), "1");
      assertEquals(stats.getVal1().get("incr_hits"), "0");

      f = client.set(k(m, "k4-"), 0, "1");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      client.incr(k(m, "k4-"), 1);
      client.incr(k(m, "k4-"), 2);
      client.incr(k(m, "k4-"), 4);
      stats = getStats(stats.getVal2(), stats.getVal3());
      assertEquals(stats.getVal1().get("incr_misses"), "1");
      assertEquals(stats.getVal1().get("incr_hits"), "3");

      client.decr(k(m, "k5-"), 1);
      stats = getStats(stats.getVal2(), stats.getVal3());
      assertEquals(stats.getVal1().get("decr_misses"), "1");
      assertEquals(stats.getVal1().get("decr_hits"), "0");

      f = client.set(k(m, "k5-"), 0, "8");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      client.decr(k(m, "k5-"), 1);
      client.decr(k(m, "k5-"), 2);
      client.decr(k(m, "k5-"), 4);
      stats = getStats(stats.getVal2(), stats.getVal3());
      assertEquals(stats.getVal1().get("decr_misses"), "1");
      assertEquals(stats.getVal1().get("decr_hits"), "3");

      client.cas(k(m, "k6-"), 1234, v(m, "v6-"));
      stats = getStats(stats.getVal2(), stats.getVal3());
      assertEquals(stats.getVal1().get("cas_misses"), "1");
      assertEquals(stats.getVal1().get("cas_hits"), "0");
      assertEquals(stats.getVal1().get("cas_badval"), "0");

      f = client.set(k(m, "k6-"), 0, v(m, "v6-"));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      CASValue value = client.gets(k(m, "k6-"));
      long old = value.getCas();
      client.cas(k(m, "k6-"), value.getCas(), v(m, "v66-"));
      stats = getStats(stats.getVal2(), stats.getVal3());
      assertEquals(stats.getVal1().get("cas_misses"), "1");
      assertEquals(stats.getVal1().get("cas_hits"), "1");
      assertEquals(stats.getVal1().get("cas_badval"), "0");
      client.cas(k(m, "k6-"), old, v(m, "v66-"));
      stats = getStats(stats.getVal2(), stats.getVal3());
      assertEquals(stats.getVal1().get("cas_misses"), "1");
      assertEquals(stats.getVal1().get("cas_hits"), "1");
      assertEquals(stats.getVal1().get("cas_badval"), "1");
   }

   private List<MemcachedClient> createMultipleClients(List<MemcachedClient> clients, int number, int from)
           throws IOException {
      if (from >= number) return clients;
      else {
         MemcachedClient newClient = createMemcachedClient(60000, server.getPort());
         Object value = newClient.get("a");
         // 'Use' the value
         if (value != null && value.hashCode() % 1000 == 0) {
            System.out.print(value.hashCode());
         }

         clients.add(newClient);
         return createMultipleClients(clients, number, from + 1);
      }
   }

   public void testStatsSpecificToMemcachedViaJmx() throws MalformedObjectNameException, AttributeNotFoundException,
           MBeanException, ReflectionException, InstanceNotFoundException, IOException {
      // Send any command
      getStats(-1, -1);

      testSingleLocalConnection(jmxDomain, "Memcached");
      List<MemcachedClient> clients = new ArrayList<>();
      try {
         clients = createMultipleClients(clients, 10, 0);
         testMultipleLocalConnections(jmxDomain, "Memcached", clients.size() + 1);
      } finally {
         clients.forEach(client -> {
            try {
               client.shutdown(20, TimeUnit.SECONDS);
            } catch (Throwable t) { } // Ignore it...
         });
      }
   }

   public void testStatsWithArgs() throws IOException {
      String resp = send("stats\r\n");
      assertExpectedResponse(resp, "STAT", false);
      resp = send("stats \r\n");
      assertExpectedResponse(resp, "STAT", false);
      resp = send("stats boo\r\n");
      assertClientError(resp);
      resp = send("stats boo boo2 boo3\r\n");
      assertClientError(resp);
   }

   private Triple<Map<String, String>, Integer, Integer> getStats(int currentBytesRead, int currentBytesWritten) {
      Map<SocketAddress, Map<String, String>> globalStats = client.getStats();
      assertEquals(globalStats.size(), 1);
      Map<String, String> stats = globalStats.values().iterator().next();
      int bytesRead = assertHigherBytes(currentBytesRead, stats.get("bytes_read"));
      int bytesWritten = assertHigherBytes(currentBytesRead, stats.get("bytes_written"));
      return new Triple(stats, bytesRead, bytesWritten);
   }

   private int assertHigherBytes(int currentBytesRead, String bytesStr) {
      int bytesRead = Integer.parseInt(bytesStr);
      assertTrue(bytesRead > currentBytesRead);
      return bytesRead;
   }
}