package org.infinispan.server.hotrod.event;

import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.HotRodMultiNodeTest;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import io.netty.channel.Channel;
import io.netty.channel.WriteBufferWaterMark;

/**
 * A primary listener coalesces the topologies queued while its channel is not writable into one event holding the
 * latest snapshot and the union of the segments whose primary owner changed in any of the superseded topologies.
 */
@Test(groups = "functional", testName = "server.hotrod.event.HotRodPrimaryNearCacheTopologyCoalescingTest")
public class HotRodPrimaryNearCacheTopologyCoalescingTest extends HotRodMultiNodeTest {

   @Override
   protected String cacheName() {
      return "primaryNearCacheTopologyCoalescingCache";
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
   }

   public void testChangedSegmentsAreUnionedAcrossSupersededTopologies() throws Exception {
      // Blocking the channel corrupts the client's stream, so it must not be a shared client
      HotRodClient client = createClient(servers().get(0), cacheName());
      EventLogListener listener = new EventLogListener();
      assertStatus(client.addPrimaryNearCacheListener(listener, 0), Success);
      try {
         Object registry = servers().get(0).getClientListenerRegistry();
         @SuppressWarnings("unchecked")
         ConcurrentMap<WrappedByteArray, Object> senders = TestingUtil.extractField(registry, "eventSenders");
         Object sender = senders.get(new WrappedByteArray(listener.getId()));
         assertNotNull(sender, "Sender must exist for the registered listener");
         Channel ch = TestingUtil.extractField(sender, "ch");
         AtomicReference<?> pendingTopology = TestingUtil.extractField(sender, "pendingTopology");

         DistributionManager dm = servers().get(0).getCacheManager().getCache(cacheName()).getAdvancedCache()
               .getDistributionManager();
         ConsistentHash all = dm.getCacheTopology().getReadConsistentHash();
         int topologyId = dm.getCacheTopology().getTopologyId();
         List<Address> members = all.getMembers();
         // The same cluster seen with only the first member owning every segment
         ConsistentHash single = DefaultConsistentHashFactory.getInstance().updateMembers(
               (org.infinispan.distribution.ch.impl.DefaultConsistentHash) all, List.of(members.get(0)), null);

         BitSet expected = new BitSet();
         for (int segment = 0; segment < all.getNumSegments(); ++segment) {
            if (!members.get(0).equals(all.locatePrimaryOwnerForSegment(segment))) {
               expected.set(segment);
            }
         }
         assertFalse(expected.isEmpty(), "The segments must be distributed over both members");

         // The registration snapshot has to be written before the channel is blocked
         eventually(() -> TestingUtil.extractField(sender, "pendingConsistentHash") == null);
         eventually(() -> pendingTopology.get() == null);
         int lastSent = (int) TestingUtil.extractField(sender, "lastSentSequence");

         Channel channel = ch;
         channel.config().setWriteBufferWaterMark(new WriteBufferWaterMark(1, 2));
         channel.eventLoop().submit(() -> channel.write(channel.alloc().buffer(64).writeZero(64))).get(5, TimeUnit.SECONDS);
         assertFalse(channel.isWritable(), "Channel should be non-writable");

         // A -> B -> A: the final owners are the same as the first, but the transition must not be forgotten
         Method queueTopology = findMethod(sender.getClass(), "queueTopology");
         queueTopology.setAccessible(true);
         queueTopology.invoke(sender, topologyId + 1, single);
         queueTopology.invoke(sender, topologyId + 2, all);

         eventually(() -> pendingTopology.get() != null && sequence(pendingTopology.get()) == topologyId + 2);
         Object coalesced = pendingTopology.get();
         assertEquals(lastSent, field(coalesced, "baseSequence"));
         assertEquals(expected, field(coalesced, "changedSegments"));
         assertTrue(ch.isOpen());

         // Writing resumes with the single latest event
         channel.config().setWriteBufferWaterMark(new WriteBufferWaterMark(32 * 1024, 64 * 1024));
         channel.eventLoop().submit(channel::flush).get(5, TimeUnit.SECONDS);
         eventually(() -> pendingTopology.get() == null);
      } finally {
         killClient(client);
      }
   }

   private static int sequence(Object tce) throws Exception {
      return (int) field(tce, "topologySequence");
   }

   private static Object field(Object tce, String name) throws Exception {
      Field field = tce.getClass().getDeclaredField(name);
      field.setAccessible(true);
      return field.get(tce);
   }

   private static Method findMethod(Class<?> clazz, String name) {
      for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
         for (Method m : c.getDeclaredMethods()) {
            if (name.equals(m.getName())) {
               return m;
            }
         }
      }
      throw new IllegalArgumentException("Method " + name + " not found in " + clazz);
   }
}
