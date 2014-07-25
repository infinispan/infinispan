package org.infinispan.partitionhandling;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.partionhandling.AvailabilityException;
import org.infinispan.partionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TEST_PING;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.View;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.Discovery;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(groups = "functional", testName = "partitionhandling.BasePartitionHandlingTest")
public class BasePartitionHandlingTest extends MultipleCacheManagersTest {


   private static Log log = LogFactory.getLog(BasePartitionHandlingTest.class);

   final static AtomicInteger viewId = new AtomicInteger(5);
   protected int numMembersInCluster = 4;
   protected CacheMode cacheMode = CacheMode.DIST_SYNC;
   protected volatile Partition[] partitions;
   protected boolean partitionHandling = true;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(cacheMode);
      dcc.clustering().partitionHandling().enabled(partitionHandling);
      createClusteredCaches(numMembersInCluster, dcc, new TransportFlags().withFD(true).withMerge(true));
      waitForClusterToForm();
   }


   @Listener
   static class ViewChangedHandler {

      volatile boolean notified = false;

      @ViewChanged
      public void viewChanged(ViewChangedEvent vce) {
         notified = true;
      }
   }

   class Partition {

      private final List<Address> allMembers;
      List<Channel> channels = new ArrayList<Channel>();

      public Partition(List<Address> allMembers) {
         this.allMembers = allMembers;
      }

      public void addNode(Channel c) {
         channels.add(c);
      }

      public void partition() {
         discardOtherMembers();

         log.trace("Partition forming");
         disableDiscovery();
         installNewView();
         assertPartitionFormed();
         assertDegradedMode();
         log.trace("New views installed");
      }

      private void disableDiscovery() {
         for (Channel c : channels) {
            for (Protocol p : c.getProtocolStack().getProtocols()) {
               if (p instanceof Discovery) {
                  if (!(p instanceof TEST_PING)) throw new IllegalStateException("TEST_PING required for this test.");
                  ((TEST_PING) p).suspend();
               }
            }
         }
      }

      private void assertPartitionFormed() {
         final List<Address> viewMembers = new ArrayList<Address>();
         for (Channel ac : channels) viewMembers.add(ac.getAddress());
         for (Channel c : channels) {
            List<Address> members = c.getView().getMembers();
            if (!members.equals(viewMembers)) throw new AssertionError();
         }
      }

      private List<Address> installNewView() {
         final List<Address> viewMembers = new ArrayList<Address>();
         for (Channel c : channels) viewMembers.add(c.getAddress());
         View view = View.create(channels.get(0).getAddress(), viewId.incrementAndGet(), (Address[]) viewMembers.toArray(new Address[viewMembers.size()]));

         log.trace("Before installing new view...");
         for (Channel c : channels)
            ((GMS) c.getProtocolStack().findProtocol(GMS.class)).installView(view);
         return viewMembers;
      }

      private void discardOtherMembers() {
         List<Address> outsideMembers = new ArrayList<Address>();
         for (Address a : allMembers) {
            boolean inThisPartition = false;
            for (Channel c : channels) {
               if (c.getAddress().equals(a)) inThisPartition = true;
            }
            if (!inThisPartition) outsideMembers.add(a);
         }
         for (Channel c : channels) {
            DISCARD discard = new DISCARD();
            for (Address a : outsideMembers) discard.addIgnoreMember(a);
            try {
               c.getProtocolStack().insertProtocol(discard, ProtocolStack.ABOVE, TP.class);
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      }

      @Override
      public String toString() {
         String addresses = "";
         for (Channel c : channels) addresses += c.getAddress() + " ";
         return "Partition{" + addresses + '}';
      }

      public void merge(Partition partition) {
         observeMembers(partition);
         partition.observeMembers(this);
         enableDiscovery();
         partition.enableDiscovery();
         channels.addAll(partition.channels);

         waitForPartitionToForm();
         disableDiscovery();
         List<Partition> tmp = new ArrayList<Partition>(Arrays.asList(BasePartitionHandlingTest.this.partitions));
         if (!tmp.remove(partition)) throw new AssertionError();
         BasePartitionHandlingTest.this.partitions = tmp.toArray(new Partition[tmp.size()]);
      }

      private void waitForPartitionToForm() {
         List<Cache<Object, Object>> caches = new ArrayList<Cache<Object, Object>>(getCaches(null));
         Iterator<Cache<Object, Object>> i = caches.iterator();
         while (i.hasNext()) {
            if (!channels.contains(channel(i.next())))
               i.remove();
         }
         Cache<Object, Object> cache = caches.get(0);
         TestingUtil.blockUntilViewsReceived(10000, caches);
         if (cache.getCacheConfiguration().clustering().cacheMode().isClustered()) {
            TestingUtil.waitForRehashToComplete(caches);
         }
      }

      public void enableDiscovery() {
         for (Channel c : channels) {
            for (Protocol p : c.getProtocolStack().getProtocols()) {
               if (p instanceof Discovery) {
                  try {
                     log.tracef("About to start discovery: %s", p);
                     p.start();
                  } catch (Exception e) {
                     throw new RuntimeException(e);
                  }
               }
            }
         }
         log.trace("Discovery started.");
      }

      private void observeMembers(Partition partition) {
         for (Channel c : channels) {
            List<Protocol> protocols = c.getProtocolStack().getProtocols();
            for (Protocol p : protocols) {
               if (p instanceof DISCARD) {
                  for (Channel oc : partition.channels) {
                     ((DISCARD) p).removeIgnoredMember(oc.getAddress());
                  }
               }
            }
         }
      }

      private void assertDegradedMode() {
         if (partitionHandling) {
            for (final Cache c : cachesInThisPartition()) {
               eventually(new Condition() {
                  @Override
                  public boolean isSatisfied() throws Exception {
                     return partitionHandlingManager(c.getAdvancedCache()).getState() == PartitionHandlingManager.PartitionState.DEGRADED_MODE;
                  }
               });
            }
         }
      }

      public void assertKeyAvailableForRead(Object k, Object expectedValue) {
         for (Cache c : cachesInThisPartition()) {
            assertEquals(c.get(k), expectedValue, "Cache " + c.getAdvancedCache().getRpcManager().getAddress() + " doesn't see the right value");
         }
      }

      public void assertKeyAvailableForWrite(Object k, Object newValue) {
         for (Cache c : cachesInThisPartition()) {
            c.put(k, newValue);
            assertEquals(c.get(k), newValue, "Cache " + c.getAdvancedCache().getRpcManager().getAddress() + " doesn't see the right value");
         }
      }

      protected void assertKeysNotAvailable(Object ... keys) {
         for (Object k : keys)
            assertKeyNotAvailable(k);
      }

      protected void assertKeyNotAvailable(Object key) {
         for (Cache c : cachesInThisPartition()) {
            try {
               c.get(key);
               fail("Key " + key + " available in cache " + address(c));
            } catch (AvailabilityException ae) {}
         }
      }


      private List<Cache> cachesInThisPartition() {
         List<Cache> caches = new ArrayList<Cache>();
         for (final Cache c : caches()) {
            if (channels.contains(channel(c))) {
               caches.add(c);
            }
         }
         return caches;
      }

      public void assertKeyNotAvailableForWrite(Object key) {
         for (Cache c : cachesInThisPartition()) {
            try {
               c.put(key, key);
               fail();
            } catch (AvailabilityException ae) {}
         }
      }

      public void assertKeysNotAvailableForWrite(Object ... keys) {
         for (Object k : keys) {
            assertKeyNotAvailableForWrite(k);
         }
      }

      public void expectPartitionState(final PartitionHandlingManager.PartitionState state) {
         for (final Cache c : cachesInThisPartition()) {
            eventually(new Condition() {
               @Override
               public boolean isSatisfied() throws Exception {
                  return partitionHandlingManager(c).getState() == state;
               }
            });
         }
      }
   }

   protected void splitCluster(int[]... parts) {
      List<Address> allMembers = channel(0).getView().getMembers();
      partitions = new Partition[parts.length];
      for (int i = 0; i < parts.length; i++) {
         Partition p = new Partition(allMembers);
         for (int j : parts[i]) {
            p.addNode(channel(j));
         }
         partitions[i] = p;
         p.partition();
      }
   }

   private Channel channel(int i) {
      Cache<Object, Object> cache = cache(i);
      return channel(cache);
   }

   private Channel channel(Cache<Object, Object> cache) {
      JGroupsTransport t = (JGroupsTransport) cache.getAdvancedCache().getRpcManager().getTransport();
      return t.getChannel();
   }

   protected Partition partition(int i) {
      if (partitions == null)
         throw new IllegalStateException("splitCluster(..) must be invoked before this method!");
      return partitions[i];
   }

   protected PartitionHandlingManager partitionHandlingManager(int index) {
      return partitionHandlingManager(advancedCache(index));
   }

   private PartitionHandlingManager partitionHandlingManager(Cache cache) {
      return cache.getAdvancedCache().getComponentRegistry().getComponent(PartitionHandlingManager.class);
   }

   protected void assertExpectedValue(int expectedVal, Object key) {
      for (int i = 0; i < 4; i++) {
         assertEquals(cache(i).get(key), expectedVal);
      }
   }

}
