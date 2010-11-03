package org.infinispan.distribution.topologyaware;

import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.NodeTopologyInfo;
import org.infinispan.distribution.ch.TopologyAwareConsistentHash;
import org.infinispan.distribution.ch.TopologyInfo;

import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.TopologyAwareConsistentHashTest")
public class TopologyAwareConsistentHashTest {
   
   private static Log log = LogFactory.getLog(TopologyAwareConsistentHashTest.class);

   TopologyInfo ti;
   TopologyAwareConsistentHash ch;
   ArrayList<Address> addresses;
   TestAddress a0;
   TestAddress a1;
   TestAddress a2;
   TestAddress a3;
   TestAddress a4;
   TestAddress a5;
   TestAddress a6;
   TestAddress a7;
   TestAddress a8;
   TestAddress a9;
   private TestAddress[] staticAddresses;


   @BeforeMethod
   public void setUp() {
      ti = new TopologyInfo();
      ch = new TopologyAwareConsistentHash();
      addresses = new ArrayList<Address>();
      for (int i = 0; i < 10; i++) {
          addresses.add(new TestAddress(i * 100));
      }
      ch.setCaches(addresses);
      addresses = new ArrayList(ch.getCaches());
      for (int i = 0; i < addresses.size(); i++) {
         TestingUtil.replaceField(addresses.get(i), "a"+i, this, TopologyAwareConsistentHashTest.class);
      }

      addresses.clear();
      ch = new TopologyAwareConsistentHash();
      ch.setTopologyInfo(ti);
   }

   public void testDifferentMachines() {
      addNode(a0, "m0", null, null);
      addNode(a1, "m1", null, null);
      addNode(a2, "m0", null, null);
      addNode(a3, "m1", null, null);
      setAddresses();

      assertLocation(ch.locate(a0, 1), true, a0);
      assertLocation(ch.locate(a1, 1), true, a1);
      assertLocation(ch.locate(a2, 1), true, a2);
      assertLocation(ch.locate(a3, 1), true, a3);

      assertLocation(ch.getStateProvidersOnLeave(a0, 1), false);
      assertLocation(ch.getStateProvidersOnLeave(a1, 1), false);
      assertLocation(ch.getStateProvidersOnLeave(a2, 1), false);
      assertLocation(ch.getStateProvidersOnLeave(a3, 1), false);

      assertLocation(ch.locate(a0, 2), true, a0, a1);
      assertLocation(ch.locate(a1, 2), true, a1, a2);
      assertLocation(ch.locate(a2, 2), true, a2, a3);
      assertLocation(ch.locate(a3, 2), true, a3, a0);
      
      assertLocation(ch.getStateProvidersOnLeave(a0, 2), false, a1, a3);
      assertLocation(ch.getStateProvidersOnLeave(a1, 2), false, a0, a2);
      assertLocation(ch.getStateProvidersOnLeave(a2, 2), false, a3, a1);
      assertLocation(ch.getStateProvidersOnLeave(a3, 2), false, a0, a2);
      


      assertLocation(ch.locate(a0, 3), true, a0, a1, a3);
      assertLocation(ch.locate(a1, 3), true, a1, a2, a0);
      assertLocation(ch.locate(a2, 3), true, a2, a3, a1);
      assertLocation(ch.locate(a3, 3), true, a3, a0, a2);

      assertLocation(ch.getStateProvidersOnLeave(a0, 3), false, a1, a3);
      assertLocation(ch.getStateProvidersOnLeave(a1, 3), false, a0, a2);
      assertLocation(ch.getStateProvidersOnLeave(a2, 3), false, a3, a1);
      assertLocation(ch.getStateProvidersOnLeave(a3, 3), false, a0, a2);
   }
   
   public void testDifferentMachines2() {
      addNode(a0, "m0", null, null);
      addNode(a1, "m0", null, null);
      addNode(a2, "m1", null, null);
      addNode(a3, "m1", null, null);
      addNode(a4, "m2", null, null);
      addNode(a5, "m2", null, null);
      setAddresses();
      assertLocation(ch.locate(a0, 1), true, a0);
      assertLocation(ch.locate(a1, 1), true, a1);
      assertLocation(ch.locate(a2, 1), true, a2);
      assertLocation(ch.locate(a3, 1), true, a3);
      assertLocation(ch.locate(a4, 1), true, a4);
      assertLocation(ch.locate(a5, 1), true, a5);

      assertLocation(ch.locate(a0, 2), true, a0, a2);
      assertLocation(ch.locate(a1, 2), true, a1, a2);
      assertLocation(ch.locate(a2, 2), true, a2, a4);
      assertLocation(ch.locate(a3, 2), true, a3, a4);
      assertLocation(ch.locate(a4, 2), true, a4, a0);
      assertLocation(ch.locate(a5, 2), true, a5, a0);

      assertLocation(ch.locate(a0, 3), true, a0, a2, a3);
      assertLocation(ch.locate(a1, 3), true, a1, a2, a3);
      assertLocation(ch.locate(a2, 3), true, a2, a4, a5);
      assertLocation(ch.locate(a3, 3), true, a3, a4, a5);
      assertLocation(ch.locate(a4, 3), true, a4, a0, a1);
      assertLocation(ch.locate(a5, 3), true, a5, a0, a1);
   }

   public void testDifferentRacksAndMachines() {
      addNode(a0, "m0", "r0", null);
      addNode(a1, "m0", "r0", null);
      addNode(a2, "m1", "r1", null);
      addNode(a3, "m2", "r2", null);
      addNode(a4, "m1", "r1", null);
      addNode(a5, "m2", "r3", null);
      setAddresses();
      assertLocation(ch.locate(a0, 1), true, a0);
      assertLocation(ch.locate(a1, 1), true, a1);
      assertLocation(ch.locate(a2, 1), true, a2);
      assertLocation(ch.locate(a3, 1), true, a3);
      assertLocation(ch.locate(a4, 1), true, a4);
      assertLocation(ch.locate(a5, 1), true, a5);

      assertLocation(ch.locate(a0, 2), true, a0, a2);
      assertLocation(ch.locate(a1, 2), true, a1, a2);
      assertLocation(ch.locate(a2, 2), true, a2, a3);
      assertLocation(ch.locate(a3, 2), true, a3, a4);
      assertLocation(ch.locate(a4, 2), true, a4, a5);
      assertLocation(ch.locate(a5, 2), true, a5, a0);

      assertLocation(ch.locate(a0, 3), true, a0, a2, a3);
      assertLocation(ch.locate(a1, 3), true, a1, a2, a3);
      assertLocation(ch.locate(a2, 3), true, a2, a3, a5);
      assertLocation(ch.locate(a3, 3), true, a3, a4, a5);
      assertLocation(ch.locate(a4, 3), true, a4, a5, a0);
      assertLocation(ch.locate(a5, 3), true, a5, a0, a1);
   }

   public void testAllSameMachine() {
      addNode(a0, "m0", null, null);
      addNode(a1, "m0", null, null);
      addNode(a2, "m0", null, null);
      addNode(a3, "m0", null, null);
      addNode(a4, "m0", null, null);
      addNode(a5, "m0", null, null);
      setAddresses();

      assertLocation(ch.locate(a0, 1), true, a0);
      assertLocation(ch.locate(a1, 1), true, a1);
      assertLocation(ch.locate(a2, 1), true, a2);
      assertLocation(ch.locate(a3, 1), true, a3);
      assertLocation(ch.locate(a4, 1), true, a4);
      assertLocation(ch.locate(a5, 1), true, a5);

      assertLocation(ch.locate(a0, 2), true, a0, a1);
      assertLocation(ch.locate(a1, 2), true, a1, a2);
      assertLocation(ch.locate(a2, 2), true, a2, a3);
      assertLocation(ch.locate(a3, 2), true, a3, a4);
      assertLocation(ch.locate(a4, 2), true, a4, a5);
      assertLocation(ch.locate(a5, 2), true, a5, a0);

      assertLocation(ch.locate(a0, 3), true, a0, a1, a2);
      assertLocation(ch.locate(a1, 3), true, a1, a2, a3);
      assertLocation(ch.locate(a2, 3), true, a2, a3, a4);
      assertLocation(ch.locate(a3, 3), true, a3, a4, a5);
      assertLocation(ch.locate(a4, 3), true, a4, a5, a0);
      assertLocation(ch.locate(a5, 3), true, a5, a0, a1);
   }

   public void testDifferentSites() {
      addNode(a0, "m0", null, "s0");
      addNode(a1, "m1", null, "s0");
      addNode(a2, "m2", null, "s1");
      addNode(a3, "m3", null, "s1");
      setAddresses();
      assertLocation(ch.locate(a0, 1), true, a0);
      assertLocation(ch.locate(a1, 1), true, a1);
      assertLocation(ch.locate(a2, 1), true, a2);
      assertLocation(ch.locate(a3, 1), true, a3);

      assertLocation(ch.locate(a0, 2), true, a0, a2);
      assertLocation(ch.locate(a1, 2), true, a1, a2);
      assertLocation(ch.locate(a2, 2), true, a2, a0);
      assertLocation(ch.locate(a3, 2), true, a3, a0);
      
      assertLocation(ch.locate(a0, 3), true, a0, a2, a3);
      assertLocation(ch.locate(a1, 3), true, a1, a2, a3);
      assertLocation(ch.locate(a2, 3), true, a2, a0, a1);
      assertLocation(ch.locate(a3, 3), true, a3, a0, a1);
   }

   public void testSitesMachines2() {
      addNode(a0, "m0", null, "s0");
      addNode(a1, "m1", null, "s1");
      addNode(a2, "m2", null, "s0");
      addNode(a3, "m3", null, "s2");
      addNode(a4, "m4", null, "s1");
      addNode(a5, "m5", null, "s1");

      setAddresses();
      assertLocation(ch.locate(a0, 1), true, a0);
      assertLocation(ch.locate(a1, 1), true, a1);
      assertLocation(ch.locate(a2, 1), true, a2);
      assertLocation(ch.locate(a3, 1), true, a3);
      assertLocation(ch.locate(a4, 1), true, a4);
      assertLocation(ch.locate(a5, 1), true, a5);

      assertLocation(ch.locate(a0, 2), true, a0, a1);
      assertLocation(ch.locate(a1, 2), true, a1, a2);
      assertLocation(ch.locate(a2, 2), true, a2, a3);
      assertLocation(ch.locate(a3, 2), true, a3, a4);
      assertLocation(ch.locate(a4, 2), true, a4, a0);
      assertLocation(ch.locate(a5, 2), true, a5, a0);

      assertLocation(ch.locate(a0, 3), true, a0, a1, a3);
      assertLocation(ch.locate(a1, 3), true, a1, a2, a3);
      assertLocation(ch.locate(a2, 3), true, a2, a3, a4);
      assertLocation(ch.locate(a3, 3), true, a3, a4, a5);
      assertLocation(ch.locate(a4, 3), true, a4, a0, a2);
      assertLocation(ch.locate(a5, 3), true, a5, a0, a2);
   }

   public void testSitesMachinesSameMachineName() {
      addNode(a0, "m0", null, "r0");
      addNode(a1, "m0", null, "r1");
      addNode(a2, "m0", null, "r0");
      addNode(a3, "m0", null, "r2");
      addNode(a4, "m0", null, "r1");
      addNode(a5, "m0", null, "r1");

      setAddresses();
      assertLocation(ch.locate(a0, 1), true, a0);
      assertLocation(ch.locate(a1, 1), true, a1);
      assertLocation(ch.locate(a2, 1), true, a2);
      assertLocation(ch.locate(a3, 1), true, a3);
      assertLocation(ch.locate(a4, 1), true, a4);
      assertLocation(ch.locate(a5, 1), true, a5);

      assertLocation(ch.locate(a0, 2), true, a0, a1);
      assertLocation(ch.locate(a1, 2), true, a1, a2);
      assertLocation(ch.locate(a2, 2), true, a2, a3);
      assertLocation(ch.locate(a3, 2), true, a3, a4);
      assertLocation(ch.locate(a4, 2), true, a4, a0);
      assertLocation(ch.locate(a5, 2), true, a5, a0);

      assertLocation(ch.locate(a0, 3), true, a0, a1, a3);
      assertLocation(ch.locate(a1, 3), true, a1, a2, a3);
      assertLocation(ch.locate(a2, 3), true, a2, a3, a4);
      assertLocation(ch.locate(a3, 3), true, a3, a4, a5);
      assertLocation(ch.locate(a4, 3), true, a4, a0, a2);
      assertLocation(ch.locate(a5, 3), true, a5, a0, a2);
   }

   public void testDifferentRacks() {
      addNode(a0, "m0", "r0", null);
      addNode(a1, "m1", "r0", null);
      addNode(a2, "m2", "r1", null);
      addNode(a3, "m3", "r1", null);
      setAddresses();
      assertLocation(ch.locate(a0, 1), true, a0);
      assertLocation(ch.locate(a1, 1), true, a1);
      assertLocation(ch.locate(a2, 1), true, a2);
      assertLocation(ch.locate(a3, 1), true, a3);

      assertLocation(ch.locate(a0, 2), true, a0, a2);
      assertLocation(ch.locate(a1, 2), true, a1, a2);
      assertLocation(ch.locate(a2, 2), true, a2, a0);
      assertLocation(ch.locate(a3, 2), true, a3, a0);

      assertLocation(ch.locate(a0, 3), true, a0, a2, a3);
      assertLocation(ch.locate(a1, 3), true, a1, a2, a3);
      assertLocation(ch.locate(a2, 3), true, a2, a0, a1);
      assertLocation(ch.locate(a3, 3), true, a3, a0, a1);
   }

   public void testRacksMachines2() {
      addNode(a0, "m0", "r0", null);
      addNode(a1, "m1", "r1", null);
      addNode(a2, "m2", "r0", null);
      addNode(a3, "m3", "r2", null);
      addNode(a4, "m4", "r1", null);
      addNode(a5, "m5", "r1", null);

      setAddresses();
      assertLocation(ch.locate(a0, 1), true, a0);
      assertLocation(ch.locate(a1, 1), true, a1);
      assertLocation(ch.locate(a2, 1), true, a2);
      assertLocation(ch.locate(a3, 1), true, a3);
      assertLocation(ch.locate(a4, 1), true, a4);
      assertLocation(ch.locate(a5, 1), true, a5);

      assertLocation(ch.locate(a0, 2), true, a0, a1);
      assertLocation(ch.locate(a1, 2), true, a1, a2);
      assertLocation(ch.locate(a2, 2), true, a2, a3);
      assertLocation(ch.locate(a3, 2), true, a3, a4);
      assertLocation(ch.locate(a4, 2), true, a4, a0);
      assertLocation(ch.locate(a5, 2), true, a5, a0);

      assertLocation(ch.locate(a0, 3), true, a0, a1, a3);
      assertLocation(ch.locate(a1, 3), true, a1, a2, a3);
      assertLocation(ch.locate(a2, 3), true, a2, a3, a4);
      assertLocation(ch.locate(a3, 3), true, a3, a4, a5);
      assertLocation(ch.locate(a4, 3), true, a4, a0, a2);
      assertLocation(ch.locate(a5, 3), true, a5, a0, a2);
   }

   public void testRacksMachinesSameMachineName() {
      addNode(a0, "m0", "r0", null);
      addNode(a1, "m0", "r1", null);
      addNode(a2, "m0", "r0", null);
      addNode(a3, "m0", "r2", null);
      addNode(a4, "m0", "r1", null);
      addNode(a5, "m0", "r1", null);

      setAddresses();
      assertLocation(ch.locate(a0, 1), true, a0);
      assertLocation(ch.locate(a1, 1), true, a1);
      assertLocation(ch.locate(a2, 1), true, a2);
      assertLocation(ch.locate(a3, 1), true, a3);
      assertLocation(ch.locate(a4, 1), true, a4);
      assertLocation(ch.locate(a5, 1), true, a5);

      assertLocation(ch.locate(a0, 2), true, a0, a1);
      assertLocation(ch.locate(a1, 2), true, a1, a2);
      assertLocation(ch.locate(a2, 2), true, a2, a3);
      assertLocation(ch.locate(a3, 2), true, a3, a4);
      assertLocation(ch.locate(a4, 2), true, a4, a0);
      assertLocation(ch.locate(a5, 2), true, a5, a0);

      assertLocation(ch.locate(a0, 3), true, a0, a1, a3);
      assertLocation(ch.locate(a1, 3), true, a1, a2, a3);
      assertLocation(ch.locate(a2, 3), true, a2, a3, a4);
      assertLocation(ch.locate(a3, 3), true, a3, a4, a5);
      assertLocation(ch.locate(a4, 3), true, a4, a0, a2);
      assertLocation(ch.locate(a5, 3), true, a5, a0, a2);
   }

   public void testComplexScenario() {
      addNode(a0, "m2", "r0", "s1");
      addNode(a1, "m1", "r0", "s0");
      addNode(a2, "m1", "r0", "s1");
      addNode(a3, "m1", "r1", "s0");
      addNode(a4, "m0", "r0", "s1");
      addNode(a5, "m0", "r1", "s1");
      addNode(a6, "m0", "r1", "s0");
      addNode(a7, "m0", "r0", "s3");
      addNode(a8, "m0", "r0", "s2");
      addNode(a9, "m0", "r0", "s0");
      setAddresses();
      
      assertLocation(ch.locate(a0, 1), true, a0);
      assertLocation(ch.locate(a1, 1), true, a1);
      assertLocation(ch.locate(a2, 1), true, a2);
      assertLocation(ch.locate(a3, 1), true, a3);
      assertLocation(ch.locate(a4, 1), true, a4);
      assertLocation(ch.locate(a5, 1), true, a5);
      assertLocation(ch.locate(a6, 1), true, a6);
      assertLocation(ch.locate(a7, 1), true, a7);
      assertLocation(ch.locate(a8, 1), true, a8);
      assertLocation(ch.locate(a9, 1), true, a9);

      assertLocation(ch.locate(a0, 2), true, a0, a1);
      assertLocation(ch.locate(a1, 2), true, a1, a2);
      assertLocation(ch.locate(a2, 2), true, a2, a3);
      assertLocation(ch.locate(a3, 2), true, a3, a4);
      assertLocation(ch.locate(a4, 2), true, a4, a6);
      assertLocation(ch.locate(a5, 2), true, a5, a6);
      assertLocation(ch.locate(a6, 2), true, a6, a7);
      assertLocation(ch.locate(a7, 2), true, a7, a8);
      assertLocation(ch.locate(a8, 2), true, a8, a9);
      assertLocation(ch.locate(a9, 2), true, a9, a0);


      assertLocation(ch.getStateProvidersOnLeave(a0, 2), false, a1, a9);
      assertLocation(ch.getStateProvidersOnLeave(a1, 2), false, a0, a2);
      assertLocation(ch.getStateProvidersOnLeave(a2, 2), false, a3, a1);
      assertLocation(ch.getStateProvidersOnLeave(a3, 2), false, a4, a2);
      assertLocation(ch.getStateProvidersOnLeave(a4, 2), false, a6, a3);
      assertLocation(ch.getStateProvidersOnLeave(a5, 2), false, a6);
      assertLocation(ch.getStateProvidersOnLeave(a6, 2), false, a4, a7, a5);
      assertLocation(ch.getStateProvidersOnLeave(a7, 2), false, a8, a6);
      assertLocation(ch.getStateProvidersOnLeave(a8, 2), false, a9, a7);
      assertLocation(ch.getStateProvidersOnLeave(a9, 2), false, a0, a8);

      assertLocation(ch.getStateProvidersOnJoin(a0, 2), false, a1, a9);
      assertLocation(ch.getStateProvidersOnJoin(a1, 2), false, a0, a2);
      assertLocation(ch.getStateProvidersOnJoin(a2, 2), false, a3, a1);
      assertLocation(ch.getStateProvidersOnJoin(a3, 2), false, a4, a2);
      assertLocation(ch.getStateProvidersOnJoin(a4, 2), false, a6, a3);
      assertLocation(ch.getStateProvidersOnJoin(a5, 2), false, a6);
      assertLocation(ch.getStateProvidersOnJoin(a6, 2), false, a4, a7, a5);
      assertLocation(ch.getStateProvidersOnJoin(a7, 2), false, a8, a6);
      assertLocation(ch.getStateProvidersOnJoin(a8, 2), false, a9, a7);
      assertLocation(ch.getStateProvidersOnJoin(a9, 2), false, a0, a8);

      
      assertLocation(ch.locate(a0, 3), true, a0, a1, a3);
      assertLocation(ch.locate(a1, 3), true, a1, a2, a4);
      assertLocation(ch.locate(a2, 3), true, a2, a3, a6);
      assertLocation(ch.locate(a3, 3), true, a3, a4, a5);
      assertLocation(ch.locate(a4, 3), true, a4, a6, a7);
      assertLocation(ch.locate(a5, 3), true, a5, a6, a7);
      assertLocation(ch.locate(a6, 3), true, a6, a7, a8);
      assertLocation(ch.locate(a7, 3), true, a7, a8, a9);
      assertLocation(ch.locate(a8, 3), true, a8, a9, a0);
      assertLocation(ch.locate(a9, 3), true, a9, a0, a2);
   }

   public void testConsistencyWhenNodeLeaves() {
      addNode(a0, "m2", "r0", "s1");
      addNode(a1, "m1", "r0", "s0");
      addNode(a2, "m1", "r0", "s1");
      addNode(a3, "m1", "r1", "s0");
      addNode(a4, "m0", "r0", "s1");
      addNode(a5, "m0", "r1", "s1");
      addNode(a6, "m0", "r1", "s0");
      addNode(a7, "m0", "r0", "s3");
      addNode(a8, "m0", "r0", "s2");
      addNode(a9, "m0", "r0", "s0");
      setAddresses();

      List<Address> a0List = ch.locate(a0, 3);
      List<Address> a1List = ch.locate(a1, 3);
      List<Address> a2List = ch.locate(a2, 3);
      List<Address> a3List = ch.locate(a3, 3);
      List<Address> a4List = ch.locate(a4, 3);
      List<Address> a5List = ch.locate(a5, 3);
      List<Address> a6List = ch.locate(a6, 3);
      List<Address> a7List = ch.locate(a7, 3);
      List<Address> a8List = ch.locate(a8, 3);
      List<Address> a9List = ch.locate(a9, 3);

      for (Address addr: addresses) {
         System.out.println("addr = " + addr);
         List<Address> addressCopy = (List<Address>) addresses.clone();
         addressCopy.remove(addr);
         ch.setCaches(addressCopy);
         checkConsistency(a0List, a0, addr, 3);
         checkConsistency(a1List, a1, addr, 3);
         checkConsistency(a2List, a2, addr, 3);
         checkConsistency(a3List, a3, addr, 3);
         checkConsistency(a4List, a4, addr, 3);
         checkConsistency(a5List, a5, addr, 3);
         checkConsistency(a6List, a6, addr, 3);
         checkConsistency(a7List, a7, addr, 3);
         checkConsistency(a8List, a8, addr, 3);
         checkConsistency(a9List, a9, addr, 3);
      }
   }

   private void checkConsistency(List<Address> a0List, TestAddress a0, Address addr, int replCount) {
      a0List = new ArrayList(a0List);
      a0List.remove(addr);      
      if (a0.equals(addr)) return;
      List<Address> currentBackupList = ch.locate(a0, replCount);
      assertEquals(replCount, currentBackupList.size(), currentBackupList.toString());
      assert currentBackupList.containsAll(a0List) : "Current backups are: " + currentBackupList + "Previous: " + a0List;
   }


   private void assertLocation(List<Address> received, boolean enforceSequence, TestAddress... expected) {
      if (expected == null) {
         assert received.isEmpty();
      }
      assertEquals(expected.length, received.size());
      if (enforceSequence) {
         assert received.equals(Arrays.asList(expected)) : "Received: " + received + " Expected: " + Arrays.toString(expected);
      } else {
         assert received.containsAll(Arrays.asList(expected)) : "Received: " + received + " Expected: " + Arrays.toString(expected);
      }
   }

   private void addNode(TestAddress address, String machineId, String rackId, String siteId) {
      addresses.add(address);
      NodeTopologyInfo nti = new NodeTopologyInfo(machineId, rackId, siteId, null);
      ti.addNodeTopologyInfo(address, nti);
   }

   private void setAddresses() {
      ch.setCaches(addresses);
      staticAddresses = new TestAddress[]{a0, a1, a2, a3, a4, a5, a6, a7, a8, a9};
      for (int i = 0; i < staticAddresses.length; i++) {
         if (staticAddresses[i] != null) staticAddresses[i].setName("a" + i);
      }
      log.info("Static addresses: " + Arrays.toString(staticAddresses));
   }

   public TestAddress address(int hashCode) {
      return new TestAddress(hashCode);
   }
}
