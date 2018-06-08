package org.infinispan.scattered;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.util.IntSet;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateProvider;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public interface ScatteredStateProvider extends StateProvider {

   void startKeysTransfer(IntSet segments, Address origin);

   /**
    * Confirm that this node has received cache topology with given topologyId and that it has
    * moved all the segments it does not own according to consistent hash
    * to the {@link ScatteredVersionManager.SegmentState#NOT_OWNED} state.
    *
    * @param topologyId
    */
   CompletableFuture<Void> confirmRevokedSegments(int topologyId);
}
