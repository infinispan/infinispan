package org.infinispan.client.hotrod;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (groups = "unit", testName = "client.hotrod.RoundRobinBalancingStrategyTest")
public class RoundRobinBalancingStrategyTest extends AbstractInfinispanTest {


   SocketAddress addr1 = new InetSocketAddress("localhost",1111);
   SocketAddress addr2 = new InetSocketAddress("localhost",2222);
   SocketAddress addr3 = new InetSocketAddress("localhost",3333);
   SocketAddress addr4 = new InetSocketAddress("localhost",4444);
   private List<SocketAddress> defaultServers;
   private RoundRobinBalancingStrategy strategy;

   @BeforeMethod
   public void setUp() {
      strategy = new RoundRobinBalancingStrategy();
      defaultServers = new ArrayList<SocketAddress>();
      defaultServers.add(addr1);
      defaultServers.add(addr2);
      defaultServers.add(addr3);
      strategy.setServers(defaultServers);
   }

   public void simpleTest() {
      // Starts with a random server
      expectServerEventually(addr1, defaultServers, null);
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr3, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr3, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr3, strategy.nextServer(null));
   }

   public void testAddServer() {
      List<SocketAddress> newServers = new ArrayList<SocketAddress>(defaultServers);
      newServers.add(addr4);
      strategy.setServers(newServers);
      // Starts with a random server
      expectServerEventually(addr1, newServers, null);
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr3, strategy.nextServer(null));
      assertEquals(addr4, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr3, strategy.nextServer(null));
      assertEquals(addr4, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr3, strategy.nextServer(null));
   }

   public void testRemoveServer() {
      List<SocketAddress> newServers = new ArrayList<SocketAddress>(defaultServers);
      newServers.remove(addr3);
      strategy.setServers(newServers);
      // Starts with a random server
      expectServerEventually(addr1, newServers, null);
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
   }

   public void testRemoveServerAfterActivity() {
      // Starts with a random server
      expectServerEventually(addr1, defaultServers, null);
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr3, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      List<SocketAddress> newServers = new ArrayList<SocketAddress>(defaultServers);
      newServers.remove(addr3);
      strategy.setServers(newServers);
      // Selects a new random server
      expectServerEventually(addr1, newServers, null);
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
   }

   public void testAddServerAfterActivity() {
      // Starts with a random server
      expectServerEventually(addr1, defaultServers, null);
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr3, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      List<SocketAddress> newServers = new ArrayList<SocketAddress>(defaultServers);
      newServers.add(addr4);
      strategy.setServers(newServers);
      // Selects a new random server
      expectServerEventually(addr3, newServers, null);
      assertEquals(addr4, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr3, strategy.nextServer(null));
      assertEquals(addr4, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr3, strategy.nextServer(null));
      assertEquals(addr4, strategy.nextServer(null));
   }

   public void testFailedServers1() {
      strategy.setServers(defaultServers);
      Set<SocketAddress> failedServers = Collections.singleton(addr1);
      // other servers should be available
      final int LOOPS = 10;
      int c1 = 0, c2 = 0;
      for (int i = 0; i < LOOPS; ++i) {
         SocketAddress server = strategy.nextServer(failedServers);
         assertNotNull(server);
         assertNotSame(addr1, server);
         if (server.equals(addr2)) {
            c1++;
         } else if (server.equals(addr3)) {
            c2++;
         }
      }
      assertEquals(LOOPS, c1 + c2);
      assertTrue(Math.abs(c1 - c2) <= 1);
   }

   public void testFailedServers2() {
      strategy.setServers(defaultServers);
      Set<SocketAddress> failedServers = new HashSet<SocketAddress>(defaultServers);
      // with all servers failed, the behaviour should be the same
      expectServerEventually(addr1, defaultServers, failedServers);
      assertEquals(addr2, strategy.nextServer(failedServers));
      assertEquals(addr3, strategy.nextServer(failedServers));
      assertEquals(addr1, strategy.nextServer(failedServers));
   }

   private void expectServerEventually(SocketAddress addr, List<SocketAddress> servers,
                                      Set<SocketAddress> failedServers) {
      for (int i = 0; i < servers.size(); i++) {
         if (addr.equals(strategy.nextServer(failedServers)))
            return;
      }
      fail("Did not get server " + addr + " after " + servers.size() + " attempts");
   }
}
