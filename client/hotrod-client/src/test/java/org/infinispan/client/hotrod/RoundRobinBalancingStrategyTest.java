package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (groups = "unit", testName = "client.hotrod.RoundRobinBalancingStrategyTest")
public class RoundRobinBalancingStrategyTest {


   InetSocketAddress addr1 = new InetSocketAddress("localhost",1111);
   InetSocketAddress addr2 = new InetSocketAddress("localhost",2222);
   InetSocketAddress addr3 = new InetSocketAddress("localhost",3333);
   InetSocketAddress addr4 = new InetSocketAddress("localhost",4444);
   private List<InetSocketAddress> defaultServers;
   private RoundRobinBalancingStrategy strategy;

   @BeforeMethod
   public void setUp() {
      strategy = new RoundRobinBalancingStrategy();
      defaultServers = new ArrayList<InetSocketAddress>();
      defaultServers.add(addr1);
      defaultServers.add(addr2);
      defaultServers.add(addr3);
      strategy.setServers(defaultServers);
   }

   public void simpleTest() {
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
      assertEquals(addr3, strategy.nextServer());
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
      assertEquals(addr3, strategy.nextServer());
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
      assertEquals(addr3, strategy.nextServer());
   }

   public void testAddServer() {
      List<InetSocketAddress> newServers = new ArrayList<InetSocketAddress>(defaultServers);
      newServers.add(addr4);
      strategy.setServers(newServers);
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
      assertEquals(addr3, strategy.nextServer());
      assertEquals(addr4, strategy.nextServer());
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
      assertEquals(addr3, strategy.nextServer());
      assertEquals(addr4, strategy.nextServer());
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
      assertEquals(addr3, strategy.nextServer());
   }

   public void testRemoveServer() {
      List<InetSocketAddress> newServers = new ArrayList<InetSocketAddress>(defaultServers);
      newServers.remove(addr3);
      strategy.setServers(newServers);
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
   }

   public void testRemoveServerAfterActivity() {
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
      assertEquals(addr3, strategy.nextServer());
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
      List<InetSocketAddress> newServers = new ArrayList<InetSocketAddress>(defaultServers);
      newServers.remove(addr3);
      strategy.setServers(newServers);
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
   }

   public void testAddServerAfterActivity() {
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
      assertEquals(addr3, strategy.nextServer());
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
      List<InetSocketAddress> newServers = new ArrayList<InetSocketAddress>(defaultServers);
      newServers.add(addr4);
      strategy.setServers(newServers);
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
      assertEquals(addr3, strategy.nextServer());
      assertEquals(addr4, strategy.nextServer());
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
      assertEquals(addr3, strategy.nextServer());
      assertEquals(addr4, strategy.nextServer());
      assertEquals(addr1, strategy.nextServer());
      assertEquals(addr2, strategy.nextServer());
      assertEquals(addr3, strategy.nextServer());
   }
}
