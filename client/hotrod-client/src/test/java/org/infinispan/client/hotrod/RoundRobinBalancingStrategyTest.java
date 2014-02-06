package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (groups = "unit", testName = "client.hotrod.RoundRobinBalancingStrategyTest")
public class RoundRobinBalancingStrategyTest {


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
      assertEquals(addr1, strategy.nextServer(null));
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
      assertEquals(addr1, strategy.nextServer(null));
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
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
   }

   public void testRemoveServerAfterActivity() {
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr3, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      List<SocketAddress> newServers = new ArrayList<SocketAddress>(defaultServers);
      newServers.remove(addr3);
      strategy.setServers(newServers);
      // the next server index is reset to 0 because it would have been out of bounds
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
   }

   public void testAddServerAfterActivity() {
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      assertEquals(addr3, strategy.nextServer(null));
      assertEquals(addr1, strategy.nextServer(null));
      assertEquals(addr2, strategy.nextServer(null));
      List<SocketAddress> newServers = new ArrayList<SocketAddress>(defaultServers);
      newServers.add(addr4);
      strategy.setServers(newServers);
      // the next server index is still valid, so it is not reset
      assertEquals(addr3, strategy.nextServer(null));
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
      assertEquals(addr1, strategy.nextServer(failedServers));
      assertEquals(addr2, strategy.nextServer(failedServers));
      assertEquals(addr3, strategy.nextServer(failedServers));
      assertEquals(addr1, strategy.nextServer(failedServers));
   }
}
