package org.infinispan.distribution;

import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.ConsistentHashBackupTest")
public class ConsistentHashBackupTest extends AbstractInfinispanTest {

   List<Address> servers;
   DefaultConsistentHash ch;

   @BeforeTest
   public void setUp() {
      servers = new LinkedList<Address>();
      int numServers = 5;
      for (int i = 0; i < numServers; i++) {
         servers.add(new TestAddress(i));
      }

      ch = (DefaultConsistentHash) BaseDistFunctionalTest.createNewConsistentHash(servers);
   }


   private List<Address> getNext(Address a, int count) {
      List<Address> addressList = ch.getAddressOnTheWheel();
      int pos = addressList.indexOf(a);
      List<Address> result = new ArrayList<Address>();
      for (int i = 1; i <= count; i++) {
         result.add(addressList.get((pos + i) % addressList.size()) );
      }
      return result;
   }

   private List<Address> getPrevious(Address a, int count) {
      List<Address> addressList = ch.getAddressOnTheWheel();
      int pos = addressList.indexOf(a);
      List<Address> result = new ArrayList<Address>();
      for (int i = 1; i <= count; i++) {
         int index;
         if (pos - i < 0) {
            index = addressList.size() + (pos - i);
         } else {
            index = pos - i;
         }
         result.add(addressList.get((index)));
      }
      return result;
   }

   public void testGetBackupsForNode() {
      TestAddress testAddress = new TestAddress(0);
      List<Address> addressList = ch.getBackupsForNode(testAddress, 2);
      assert addressList.size() == 1;

      assert addressList.get(0).equals(getNext(testAddress, 1).get(0));

      TestAddress ta2 = new TestAddress(3);
      addressList = ch.getBackupsForNode(ta2, 3);
      assert addressList.size() == 2;

      assert addressList.equals(getNext(ta2, 2));
   }

   public void testNodesBackup() {
      TestAddress testAddress = new TestAddress(0);
      List<Address> addressList = ch.getNodesThatBackupHere(testAddress, 2);
      assert addressList.size() == 1;
      assert addressList.get(0).equals(getPrevious(testAddress, 1).get(0));

      TestAddress ta2 = new TestAddress(3);
      addressList = ch.getNodesThatBackupHere(ta2, 3);
      assert addressList.size() == 2;

      List<Address> prev = getPrevious(ta2, 2);
      assert addressList.containsAll(prev);
      assert prev.containsAll(addressList);
   }   
}
