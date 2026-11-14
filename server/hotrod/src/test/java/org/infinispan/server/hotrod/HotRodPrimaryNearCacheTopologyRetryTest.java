package org.infinispan.server.hotrod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.topology.CacheTopology;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link Encoder2x#generateTopologyResponse(short, int, Map, CacheMode, CacheTopology)}, the logic
 * that decides which Hot Rod topology update (if any) is sent to a client when the topology (address) cache is
 * still incomplete, i.e. one or more cache members have not registered their Hot Rod endpoint address yet.
 * <p>
 * The expected behaviour is:
 * <ul>
 *    <li>Address cache incomplete <em>and</em> a rebalance is in progress: postpone the update and retry on the
 *    next request (the pending consistent hash remains in place).</li>
 *    <li>Address cache incomplete and <em>no</em> rebalance in progress (e.g. a storage-only node): send a
 *    partial update with the known endpoints, using a topology id that won't suppress a later full update.</li>
 *    <li>Address cache complete: send the full topology update.</li>
 * </ul>
 * <p>
 * The test lives in the {@code org.infinispan.server.hotrod} package (next to the class under test) so it can
 * exercise the package-private decision method and the package-private topology response types directly.
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodPrimaryNearCacheTopologyRetryTest")
public class HotRodPrimaryNearCacheTopologyRetryTest extends AbstractInfinispanTest {

   private static final DefaultConsistentHashFactory CHF = DefaultConsistentHashFactory.getInstance();
   private static final int NUM_OWNERS = 2;
   private static final int NUM_SEGMENTS = 16;

   private final Address a = Address.random("a");
   private final Address b = Address.random("b");
   private final Address c = Address.random("c");

   private DefaultConsistentHash ch(Address... members) {
      return CHF.create(NUM_OWNERS, NUM_SEGMENTS, List.of(members), null);
   }

   private CacheTopology stableTopology(DefaultConsistentHash currentCH) {
      return new CacheTopology(5, 1, currentCH, null, null, CacheTopology.Phase.NO_REBALANCE,
            currentCH.getMembers(), null);
   }

   private CacheTopology rebalancingTopology(DefaultConsistentHash currentCH, DefaultConsistentHash pendingCH) {
      DefaultConsistentHash unionCH = CHF.union(currentCH, pendingCH);
      return new CacheTopology(6, 2, currentCH, pendingCH, unionCH, CacheTopology.Phase.READ_OLD_WRITE_ALL,
            unionCH.getMembers(), null);
   }

   private ServerAddress endpoint(String host) {
      return ServerAddress.forAddress(host, 11222, false);
   }

   private Map<Address, ServerAddress> endpoints(Address... members) {
      Map<Address, ServerAddress> map = new HashMap<>();
      for (Address member : members) {
         map.put(member, endpoint(member.toString()));
      }
      return map;
   }

   private Optional<AbstractTopologyResponse> response(int clientTopologyId, Map<Address, ServerAddress> endpoints,
         CacheMode cacheMode, CacheTopology topology) {
      return Encoder2x.generateTopologyResponse(Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE, clientTopologyId,
            endpoints, cacheMode, topology);
   }

   /**
    * Reproducer for the reported failure: while the address cache is incomplete during a rebalance, the server
    * must postpone the topology update (so the client retries) and must keep the pending consistent hash in
    * place so it is eventually able to deliver the final topology.
    */
   public void testTopologyRetriesWhenAddressCacheIncomplete() {
      // Node c is joining (present in the pending CH) but has not registered its Hot Rod endpoint yet.
      CacheTopology topology = rebalancingTopology(ch(a, b), ch(a, b, c));
      Map<Address, ServerAddress> incompleteAddressCache = endpoints(a, b);

      Optional<AbstractTopologyResponse> response =
            response(-1, incompleteAddressCache, CacheMode.DIST_SYNC, topology);

      assertTrue(response.isEmpty(),
            "topology update must be postponed (retried) while the address cache is incomplete during a rebalance");
      assertNotNull(topology.getPendingCH(),
            "pendingConsistentHash must still be set while address cache is incomplete");
   }

   /**
    * Once the joining node registers its endpoint, the full topology update is delivered.
    */
   public void testFullTopologyDeliveredAfterAddressCacheCompletes() {
      CacheTopology topology = rebalancingTopology(ch(a, b), ch(a, b, c));
      Map<Address, ServerAddress> completeAddressCache = endpoints(a, b, c);

      Optional<AbstractTopologyResponse> response =
            response(-1, completeAddressCache, CacheMode.DIST_SYNC, topology);

      assertTrue(response.isPresent(), "a full topology update must be sent once the address cache is complete");
      AbstractTopologyResponse topologyResponse = response.get();
      assertEquals(topology.getReadConsistentHash().hashCode(), topologyResponse.topologyId);
      assertEquals(completeAddressCache.keySet(), topologyResponse.serverEndpointsMap.keySet());
   }

   /**
    * With no rebalance in progress, a member that will never register an endpoint (a storage-only node) must not
    * block the topology update forever: a partial update with the known endpoints is sent instead.
    */
   public void testPartialTopologyWhenMemberHasNoEndpoint() {
      // a, b and c are all cache members, but c is a storage-only node without a Hot Rod endpoint.
      CacheTopology topology = stableTopology(ch(a, b, c));
      Map<Address, ServerAddress> addressCache = endpoints(a, b);

      Optional<AbstractTopologyResponse> response =
            response(-1, addressCache, CacheMode.DIST_SYNC, topology);

      assertTrue(response.isPresent(), "a partial topology update must be sent for storage-only members");
      AbstractTopologyResponse topologyResponse = response.get();
      assertEquals(Map.of(a, endpoint(a.toString()), b, endpoint(b.toString())).keySet(),
            topologyResponse.serverEndpointsMap.keySet());
      assertNotEquals(topology.getReadConsistentHash().hashCode(), topologyResponse.topologyId,
            "a partial topology id must differ from the full topology id so a later full update is not suppressed");
   }

   /**
    * A client that already reported the partial topology id must get no further update until something changes.
    */
   public void testPartialTopologyUpdateIsIdempotent() {
      CacheTopology topology = stableTopology(ch(a, b, c));
      Map<Address, ServerAddress> addressCache = endpoints(a, b);

      int partialTopologyId = response(-1, addressCache, CacheMode.DIST_SYNC, topology).get().topologyId;

      Optional<AbstractTopologyResponse> repeated =
            response(partialTopologyId, endpoints(a, b), CacheMode.DIST_SYNC, topology);

      assertTrue(repeated.isEmpty(), "a client that already has the partial topology must not be updated again");
   }

   /**
    * After a client received a partial update, the full update is still delivered once the storage-only node is
    * no longer a member (address cache becomes complete), proving the partial id does not suppress it.
    */
   public void testFullUpdateFollowsPartialUpdate() {
      CacheTopology partialTopology = stableTopology(ch(a, b, c));
      int partialTopologyId = response(-1, endpoints(a, b), CacheMode.DIST_SYNC, partialTopology).get().topologyId;

      CacheTopology completeTopology = stableTopology(ch(a, b));
      Optional<AbstractTopologyResponse> response =
            response(partialTopologyId, endpoints(a, b), CacheMode.DIST_SYNC, completeTopology);

      assertTrue(response.isPresent(), "a full update must follow a partial update once the topology is complete");
      assertEquals(completeTopology.getReadConsistentHash().hashCode(), response.get().topologyId);
   }

   /**
    * If no endpoint is known yet for the current members, there is nothing to send, so the update is postponed.
    */
   public void testNoKnownEndpointsPostpones() {
      CacheTopology topology = stableTopology(ch(a, b));
      Map<Address, ServerAddress> emptyAddressCache = new HashMap<>();

      Optional<AbstractTopologyResponse> response =
            response(-1, emptyAddressCache, CacheMode.DIST_SYNC, topology);

      assertTrue(response.isEmpty(), "topology update must be postponed when no endpoints are known");
   }

   /**
    * A complete address cache during a rebalance (all current and joining members registered) yields a full
    * topology update rather than a postpone.
    */
   public void testCompleteDuringRebalanceSendsFullTopology() {
      CacheTopology topology = rebalancingTopology(ch(a, b), ch(a, b, c));
      Map<Address, ServerAddress> completeAddressCache = endpoints(a, b, c);

      Optional<AbstractTopologyResponse> response =
            response(-1, completeAddressCache, CacheMode.DIST_SYNC, topology);

      assertTrue(response.isPresent());
      assertEquals(completeAddressCache.keySet(), response.get().serverEndpointsMap.keySet());
   }

   /**
    * A hash-distribution-aware client on a distributed cache receives a {@link HashDistAware20Response}.
    */
   public void testHashDistributionAwareClientGetsHashResponse() {
      CacheTopology topology = stableTopology(ch(a, b));

      Optional<AbstractTopologyResponse> response =
            response(-1, endpoints(a, b), CacheMode.DIST_SYNC, topology);

      assertTrue(response.isPresent());
      assertInstanceOf(HashDistAware20Response.class, response.get());
   }

   /**
    * On an invalidation cache, even a hash-distribution-aware client receives a plain topology-aware response.
    */
   public void testInvalidationModeReturnsTopologyAwareResponse() {
      CacheTopology topology = stableTopology(ch(a, b));

      Optional<AbstractTopologyResponse> response =
            response(-1, endpoints(a, b), CacheMode.INVALIDATION_SYNC, topology);

      assertTrue(response.isPresent());
      assertInstanceOf(TopologyAwareResponse.class, response.get());
      assertFalse(response.get() instanceof HashDistAware20Response);
   }

   /**
    * A topology-aware (non hash) client always receives a {@link TopologyAwareResponse}.
    */
   public void testTopologyAwareClientGetsTopologyAwareResponse() {
      CacheTopology topology = stableTopology(ch(a, b));

      Optional<AbstractTopologyResponse> response = Encoder2x.generateTopologyResponse(
            Constants.INTELLIGENCE_TOPOLOGY_AWARE, -1, endpoints(a, b), CacheMode.DIST_SYNC, topology);

      assertTrue(response.isPresent());
      assertInstanceOf(TopologyAwareResponse.class, response.get());
      assertFalse(response.get() instanceof HashDistAware20Response);
   }
}
