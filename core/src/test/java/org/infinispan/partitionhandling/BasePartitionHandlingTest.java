package org.infinispan.partitionhandling;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.AbstractDelegatingTransport;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TEST_PING;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MergeView;
import org.jgroups.View;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.Discovery;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.BasePartitionHandlingTest")
public class BasePartitionHandlingTest extends MultipleCacheManagersTest {
   private static Log log = LogFactory.getLog(BasePartitionHandlingTest.class);

   private final AtomicInteger viewId = new AtomicInteger(5);
   protected int numMembersInCluster = 4;
   protected CacheMode cacheMode = CacheMode.DIST_SYNC;
   protected volatile Partition[] partitions;
   protected boolean partitionHandling = true;

   public BasePartitionHandlingTest() {
      this.cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = new ConfigurationBuilder();
      dcc.clustering().cacheMode(cacheMode).partitionHandling().enabled(partitionHandling);
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

   public static class PartitionDescriptor {
      int[] nodes;

      public PartitionDescriptor(int... nodes) {
         this.nodes = nodes;
      }

      public int[] getNodes() {
         return nodes;
      }

      public int node(int i) {
         return nodes[i];
      }
   }

   public class Partition {

      private final List<Address> allMembers;
      List<JChannel> channels = new ArrayList<>();

      public Partition(List<Address> allMembers) {
         this.allMembers = allMembers;
      }

      public void addNode(JChannel c) {
         channels.add(c);
      }

      public void partition() {
         discardOtherMembers();

         log.trace("Partition forming");
         disableDiscovery();
         installNewView();
         assertPartitionFormed();
         log.trace("New views installed");
      }

      private void disableDiscovery() {
         for (JChannel c : channels) {
            for (Protocol p : c.getProtocolStack().getProtocols()) {
               if (p instanceof Discovery) {
                  if (!(p instanceof TEST_PING)) throw new IllegalStateException("TEST_PING required for this test.");
                  ((TEST_PING) p).suspend();
               }
            }
         }
      }

      private void assertPartitionFormed() {
         final List<Address> viewMembers = new ArrayList<>();
         for (JChannel ac : channels) viewMembers.add(ac.getAddress());
         for (JChannel c : channels) {
            List<Address> members = c.getView().getMembers();
            if (!members.equals(viewMembers)) throw new AssertionError();
         }
      }

      private List<Address> installNewView() {
         final List<Address> viewMembers = new ArrayList<>();
         for (JChannel c : channels) viewMembers.add(c.getAddress());
         View view = View.create(channels.get(0).getAddress(), viewId.incrementAndGet(), (Address[]) viewMembers.toArray(new Address[viewMembers.size()]));

         log.trace("Before installing new view...");
         for (JChannel c : channels)
            ((GMS) c.getProtocolStack().findProtocol(GMS.class)).installView(view);
         return viewMembers;
      }

      private List<Address> installMergeView(ArrayList<JChannel> view1, ArrayList<JChannel> view2) {
         List<Address> allAddresses =
               Stream.concat(view1.stream(), view2.stream()).map(JChannel::getAddress).distinct()
                     .collect(Collectors.toList());

         View v1 = toView(view1);
         View v2 = toView(view2);
         List<View> allViews = new ArrayList<>();
         allViews.add(v1);
         allViews.add(v2);

//         log.trace("Before installing new view: " + viewMembers);
//         System.out.println("Before installing new view: " + viewMembers);
         MergeView mv = new MergeView(view1.get(0).getAddress(), (long)viewId.incrementAndGet(), allAddresses, allViews);
         for (JChannel c : channels)
            ((GMS) c.getProtocolStack().findProtocol(GMS.class)).installView(mv);
         return allMembers;
      }

      private View toView(ArrayList<JChannel> channels) {
         final List<Address> viewMembers = new ArrayList<>();
         for (JChannel c : channels) viewMembers.add(c.getAddress());
         return View.create(channels.get(0).getAddress(), viewId.incrementAndGet(), (Address[]) viewMembers.toArray(new Address[viewMembers.size()]));
      }

      private void discardOtherMembers() {
         List<Address> outsideMembers = new ArrayList<>();
         for (Address a : allMembers) {
            boolean inThisPartition = false;
            for (JChannel c : channels) {
               if (c.getAddress().equals(a)) inThisPartition = true;
            }
            if (!inThisPartition) outsideMembers.add(a);
         }
         for (JChannel c : channels) {
            DISCARD discard = new DISCARD();
            for (Address a : outsideMembers) discard.addIgnoreMember(a);
            try {
               c.getProtocolStack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      }

      @Override
      public String toString() {
         String addresses = "";
         for (JChannel c : channels) addresses += c.getAddress() + " ";
         return "Partition{" + addresses + '}';
      }

      public void merge(Partition partition) {
         observeMembers(partition);
         partition.observeMembers(this);
         ArrayList<JChannel> view1 = new ArrayList<>(channels);
         ArrayList<JChannel> view2 = new ArrayList<>(partition.channels);
//         System.out.println("view1 = " + printView(view1));
//         System.out.println("view2 = " + printView(view2));
         partition.channels.stream().filter(c -> !channels.contains(c)).forEach(c -> channels.add(c));
         installMergeView(view1, view2);
         waitForPartitionToForm();
         List<Partition> tmp = new ArrayList<>(Arrays.asList(BasePartitionHandlingTest.this.partitions));
         if (!tmp.remove(partition)) throw new AssertionError();
         BasePartitionHandlingTest.this.partitions = tmp.toArray(new Partition[tmp.size()]);
      }

      private String printView(ArrayList<JChannel> view1) {
         StringBuilder sb = new StringBuilder();
         for (JChannel c: view1) sb.append(c.getAddress()).append(" ");
         return sb.insert(0, "[ ").append(" ]").toString();
      }

      private void waitForPartitionToForm() {
         List<Cache<Object, Object>> caches = new ArrayList<>(getCaches(null));
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
         for (JChannel c : channels) {
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
         for (JChannel c : channels) {
            List<Protocol> protocols = c.getProtocolStack().getProtocols();
            for (Protocol p : protocols) {
               if (p instanceof DISCARD) {
                  for (JChannel oc : partition.channels) {
                     ((DISCARD) p).removeIgnoredMember(oc.getAddress());
                  }
               }
            }
         }
      }

      public void assertDegradedMode() {
         if (partitionHandling) {
            assertAvailabilityMode(AvailabilityMode.DEGRADED_MODE);
         }
      }

      public void assertKeyAvailableForRead(Object k, Object expectedValue) {
         for (Cache c : cachesInThisPartition()) {
            assertEquals(c.get(k), expectedValue, "Cache " + c.getAdvancedCache().getRpcManager().getAddress() + " doesn't see the right value: ");
         }
      }

      public void assertKeyAvailableForWrite(Object k, Object newValue) {
         for (Cache<Object, Object> c : cachesInThisPartition()) {
            c.put(k, newValue);
            assertEquals(c.get(k), newValue, "Cache " + c.getAdvancedCache().getRpcManager().getAddress() + " doesn't see the right value");
         }
      }

      protected void assertKeysNotAvailableForRead(Object... keys) {
         for (Object k : keys)
            assertKeyNotAvailableForRead(k);
      }

      protected void assertKeyNotAvailableForRead(Object key) {
         for (Cache<Object, ?> c : cachesInThisPartition()) {
            try {
               c.get(key);
               fail("Key " + key + " available in cache " + address(c));
            } catch (AvailabilityException ae) {
               //expected!
            }
         }
      }


      private <K,V> List<Cache<K,V>> cachesInThisPartition() {
         List<Cache<K,V>> caches = new ArrayList<>();
         for (final Cache<K,V> c : BasePartitionHandlingTest.this.<K,V>caches()) {
            if (channels.contains(channel(c))) {
               caches.add(c);
            }
         }
         return caches;
      }

      public void assertKeyNotAvailableForWrite(Object key) {
         for (Cache<Object, Object> c : cachesInThisPartition()) {
            try {
               c.put(key, key);
               fail();
            } catch (AvailabilityException ae) {
               //expected!
            }
         }
      }

      public void assertKeysNotAvailableForWrite(Object ... keys) {
         for (Object k : keys) {
            assertKeyNotAvailableForWrite(k);
         }
      }

      public void assertAvailabilityMode(final AvailabilityMode state) {
         for (final Cache c : cachesInThisPartition()) {
            eventuallyEquals(state, () -> partitionHandlingManager(c).getAvailabilityMode());
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

   protected void isolatePartition(int[] isolatedPartition) {
      List<Address> allMembers = channel(0).getView().getMembers();
      Partition p0 = new Partition(allMembers);
      IntStream.range(0, allMembers.size()).forEach(i -> p0.addNode(channel(i)));

      Partition p1 = new Partition(allMembers);
      Arrays.stream(isolatedPartition).forEach(i -> p1.addNode(channel(i)));
      p1.partition();
      partitions = new Partition[]{p0, p1};
   }


   private JChannel channel(int i) {
      return channel(cache(i));
   }

   private JChannel channel(Cache<?, ?> cache) {
      return extractJGroupsTransport(cache.getAdvancedCache().getRpcManager().getTransport()).getChannel();
   }

   protected Partition partition(int i) {
      if (partitions == null)
         throw new IllegalStateException("splitCluster(..) must be invoked before this method!");
      return partitions[i];
   }

   protected PartitionHandlingManager partitionHandlingManager(int index) {
      return partitionHandlingManager(advancedCache(index));
   }

   protected PartitionHandlingManager partitionHandlingManager(Cache cache) {
      return cache.getAdvancedCache().getComponentRegistry().getComponent(PartitionHandlingManager.class);
   }

   protected void assertExpectedValue(Object expectedVal, Object key) {
      for (int i = 0; i < numMembersInCluster; i++) {
         assertEquals(cache(i).get(key), expectedVal);
      }
   }

   private static JGroupsTransport extractJGroupsTransport(Transport transport) {
      if (transport instanceof AbstractDelegatingTransport) {
         return extractJGroupsTransport(((AbstractDelegatingTransport) transport).getDelegate());
      } else if (transport instanceof JGroupsTransport) {
         return (JGroupsTransport) transport;
      }
      throw new IllegalArgumentException("Transport is not a JGroupsTransport! It is " + transport.getClass());
   }

}
