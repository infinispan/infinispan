package org.infinispan.partitionhandling;

import static org.infinispan.test.Exceptions.expectException;
import static org.testng.Assert.assertEquals;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.context.Flag;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.AbstractDelegatingTransport;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.Exceptions;
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
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.BasePartitionHandlingTest")
public class BasePartitionHandlingTest extends MultipleCacheManagersTest {
   protected static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final AtomicInteger viewId = new AtomicInteger(5);
   protected int numMembersInCluster = 4;
   protected int numberOfOwners = 2;
   protected volatile Partition[] partitions;
   protected PartitionHandling partitionHandling = PartitionHandling.DENY_READ_WRITES;
   protected EntryMergePolicy<String, String> mergePolicy = null;

   public BasePartitionHandlingTest() {
      this.cacheMode = CacheMode.DIST_SYNC;
      this.cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = cacheConfiguration();
      dcc.clustering().cacheMode(cacheMode).partitionHandling().whenSplit(partitionHandling).mergePolicy(mergePolicy);
      if (cacheMode == CacheMode.DIST_SYNC) {
         dcc.clustering().hash().numOwners(numberOfOwners);
      }
      if (biasAcquisition != null) {
         dcc.clustering().biasAcquisition(biasAcquisition);
      }
      createClusteredCaches(numMembersInCluster, dcc, new TransportFlags().withFD(true).withMerge(true));
      waitForClusterToForm();
   }

   protected BasePartitionHandlingTest partitionHandling(PartitionHandling partitionHandling) {
      this.partitionHandling = partitionHandling;
      return this;
   }

   protected String[] parameterNames() {
      return new String[]{ null, "tx", "locking", "TO", "isolation", "bias", "triangle", null };
   }

   protected Object[] parameterValues() {
      return new Object[]{ cacheMode, transactional, lockingMode, totalOrder, isolationLevel, biasAcquisition, useTriangle, partitionHandling};
   }

   protected ConfigurationBuilder cacheConfiguration() {
      return new ConfigurationBuilder();
   }

   @Listener
   public static class ViewChangedHandler {

      private volatile boolean notified = false;

      public boolean isNotified() {
         return notified;
      }

      public void setNotified(boolean notified) {
         this.notified = notified;
      }

      @ViewChanged
      public void viewChanged(ViewChangedEvent vce) {
         notified = true;
      }
   }

   public static class PartitionDescriptor {
      int[] nodes;
      AvailabilityMode expectedMode;

      public PartitionDescriptor(int... nodes) {
         this(null, nodes);
      }

      public PartitionDescriptor(AvailabilityMode expectedMode, int... nodes) {
         this.expectedMode = expectedMode;
         this.nodes = nodes;
      }

      public int[] getNodes() {
         return nodes;
      }

      public int node(int i) {
         return nodes[i];
      }

      public void assertAvailabilityMode(Partition partition) {
         partition.assertAvailabilityMode(expectedMode);
      }

      public AvailabilityMode getExpectedMode() {
         return expectedMode;
      }

      @Override
      public String toString() {
         return Arrays.toString(nodes);
      }
   }

   protected void disableDiscoveryProtocol(JChannel c) {
      ((TEST_PING)c.getProtocolStack().findProtocol(TEST_PING.class)).suspend();
   }

   protected void enableDiscoveryProtocol(JChannel c) {
      try {
         c.getProtocolStack().findProtocol(TEST_PING.class).start();
      } catch (Exception e) {
         throw new RuntimeException(e);
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
         channels.forEach(BasePartitionHandlingTest.this::disableDiscoveryProtocol);
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
         merge(partition, true);
      }

      public void merge(Partition partition, boolean waitForNoRebalance) {
         observeMembers(partition);
         partition.observeMembers(this);
         ArrayList<JChannel> view1 = new ArrayList<>(channels);
         ArrayList<JChannel> view2 = new ArrayList<>(partition.channels);
         partition.channels.stream().filter(c -> !channels.contains(c)).forEach(c -> channels.add(c));
         installMergeView(view1, view2);
         waitForPartitionToForm(waitForNoRebalance);
         List<Partition> tmp = new ArrayList<>(Arrays.asList(BasePartitionHandlingTest.this.partitions));
         if (!tmp.remove(partition)) throw new AssertionError();
         BasePartitionHandlingTest.this.partitions = tmp.toArray(new Partition[tmp.size()]);
      }

      private String printView(ArrayList<JChannel> view1) {
         StringBuilder sb = new StringBuilder();
         for (JChannel c: view1) sb.append(c.getAddress()).append(" ");
         return sb.insert(0, "[ ").append(" ]").toString();
      }

      private void waitForPartitionToForm(boolean waitForNoRebalance) {
         List<Cache<Object, Object>> caches = new ArrayList<>(getCaches(null));
         Iterator<Cache<Object, Object>> i = caches.iterator();
         while (i.hasNext()) {
            if (!channels.contains(channel(i.next())))
               i.remove();
         }
         Cache<Object, Object> cache = caches.get(0);
         TestingUtil.blockUntilViewsReceived(10000, caches);

         if (waitForNoRebalance) {
            if (cache.getCacheConfiguration().clustering().cacheMode().isClustered()) {
               TestingUtil.waitForNoRebalance(caches);
            }
         }
      }

      public void enableDiscovery() {
         channels.forEach(BasePartitionHandlingTest.this::enableDiscoveryProtocol);
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
         if (partitionHandling != PartitionHandling.ALLOW_READ_WRITES) {
            assertAvailabilityMode(AvailabilityMode.DEGRADED_MODE);
         }
         // Keys do not become unavailable immediately after the partition becomes degraded
         // but only after the cache topology is updated so that the key owners are not actual members
         assertActualMembers();
      }

      public void assertKeyAvailableForRead(Object k, Object expectedValue) {
         for (Cache c : cachesInThisPartition()) {
            BasePartitionHandlingTest.this.assertKeyAvailableForRead(c, k, expectedValue);
            // While we keep the null values in the map inside interceptor stack, these are removed in CacheImpl.getAll
            Map<Object, Object> expectedMap = expectedValue == null ? Collections.emptyMap() : Collections.singletonMap(k, expectedValue);
            assertEquals(c.getAdvancedCache().getAll(Collections.singleton(k)), expectedMap, "Cache " + c.getAdvancedCache().getRpcManager().getAddress() + " doesn't see the right value: ");
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

      public void assertKeyNotAvailableForRead(Object key) {
         for (Cache<Object, ?> c : cachesInThisPartition()) {
            BasePartitionHandlingTest.this.assertKeyNotAvailableForRead(c, key);
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

      public void assertExceptionWithForceLock(Object key) {
         cachesInThisPartition().forEach(c -> Exceptions.expectException(AvailabilityException.class,
               () -> c.getAdvancedCache().withFlags(Flag.FORCE_WRITE_LOCK).get(key)));
      }

      public void assertKeyNotAvailableForWrite(Object key) {
         cachesInThisPartition().forEach(c -> Exceptions.expectException(AvailabilityException.class, () -> c.put(key, key)));
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

      public void assertConsistentHashMembers(List<org.infinispan.remoting.transport.Address> expectedMembers) {
         for (Cache c : cachesInThisPartition()) {
            assertEquals(new HashSet<>(c.getAdvancedCache().getDistributionManager().getCacheTopology().getMembers()), new HashSet<>(expectedMembers));
         }
      }

      public void assertActualMembers() {
         Set<org.infinispan.remoting.transport.Address> expected =
            cachesInThisPartition().stream()
                                   .map(c -> c.getAdvancedCache().getRpcManager().getAddress())
                                   .collect(Collectors.toSet());
         for (Cache c : cachesInThisPartition()) {
            eventuallyEquals(expected, () -> new HashSet<>(c.getAdvancedCache().getDistributionManager().getCacheTopology().getActualMembers()));
         }
      }

      public List<org.infinispan.remoting.transport.Address> getAddresses() {
         return channels.stream().map(ch -> new JGroupsAddress(ch.getAddress())).collect(Collectors.toList());
      }
   }

   protected void assertKeyAvailableForRead(Cache c, Object k, Object expectedValue) {
      log.tracef("Checking key is available on %s", c);
      assertEquals(c.get(k), expectedValue, "Cache " + c.getAdvancedCache().getRpcManager().getAddress() + " doesn't see the right value: ");
   }

   protected void assertKeyNotAvailableForRead(Cache<Object, ?> c, Object key) {
      log.tracef("Checking key is not available on %s", c);
      expectException(AvailabilityException.class, () -> c.get(key));
      expectException(AvailabilityException.class, () -> c.getAdvancedCache().getAll(Collections.singleton(key)));
   }


   protected void splitCluster(PartitionDescriptor... partitions) {
      splitCluster(Arrays.stream(partitions).map(p -> p.getNodes()).toArray(int[][]::new));
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

   protected AdvancedCache[] getPartitionCaches(PartitionDescriptor descriptor) {
      int[] nodes = descriptor.getNodes();
      AdvancedCache[] caches = new AdvancedCache[nodes.length];
      for (int i = 0; i < nodes.length; i++)
         caches[i] = advancedCache(nodes[i]);
      return caches;
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

   protected JChannel channel(Cache<?, ?> cache) {
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
