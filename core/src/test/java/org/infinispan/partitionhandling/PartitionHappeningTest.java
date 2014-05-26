package org.infinispan.partitionhandling;

import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "partitionhandling.PartitionHappeningTest")
public class PartitionHappeningTest extends BasePartitionHandlingTest {

   public PartitionHappeningTest() {
      partitionHandling = false;
   }

   public void testPartitionHappening() throws Throwable {

      final List<ViewChangedHandler> listeners = new ArrayList<ViewChangedHandler>();
      for (int i = 0; i < caches().size(); i++) {
         ViewChangedHandler listener = new ViewChangedHandler();
         cache(i).getCacheManager().addListener(listener);
         listeners.add(listener);
      }

      splitCluster(new int[]{0, 1}, new int[]{2, 3});

      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            for (ViewChangedHandler l : listeners)
               if (!l.notified) return false;
            return true;
         }
      });

      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            RpcManager rpcManager = advancedCache(0).getRpcManager();
            List<org.infinispan.remoting.transport.Address> members = rpcManager.getTransport().getMembers();
            return members.size() == 2;
         }
      });
      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return clusterAndChFormed(0, 2);
         }
      });
      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return clusterAndChFormed(1, 2);
         }
      });
      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return clusterAndChFormed(2, 2);
         }
      });
      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return clusterAndChFormed(3, 2);
         }
      });


      cache(0).put("k", "v1");
      cache(2).put("k", "v2");

      assertEquals(cache(0).get("k"), "v1");
      assertEquals(cache(1).get("k"), "v1");
      assertEquals(cache(2).get("k"), "v2");
      assertEquals(cache(3).get("k"), "v2");

      partition(0).merge(partition(1));

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return clusterAndChFormed(0, 4);
         }
      });

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return clusterAndChFormed(1, 4);
         }
      });

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return clusterAndChFormed(2, 4);
         }
      });

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return clusterAndChFormed(3, 4);
         }
      });
   }

   public boolean clusterAndChFormed(int cacheIndex, int memberCount) {
      return advancedCache(cacheIndex).getRpcManager().getTransport().getMembers().size() == memberCount &&
            advancedCache(cacheIndex).getDistributionManager().getConsistentHash().getMembers().size() == memberCount;
   }

}
