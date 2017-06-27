package org.infinispan.scattered;

import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateProvider;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public interface ScatteredStateProvider extends StateProvider {

   void startKeysTransfer(Set<Integer> segments, Address origin);

   /**
    * Confirm that this node has received cache topology with given topologyId and that it has
    * moved all the segments it does not own according to consistent hash
    * to the {@link ScatteredVersionManager.SegmentState#NOT_OWNED} state.
    *
    * @param topologyId
    */
   CompletableFuture<Void> confirmRevokedSegments(int topologyId);
}
