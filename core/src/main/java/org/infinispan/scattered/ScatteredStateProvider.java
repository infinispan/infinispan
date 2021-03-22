package org.infinispan.scattered;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.IntSet;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateProvider;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public interface ScatteredStateProvider extends StateProvider {

   /**
    * Start transferring keys and remote metadata for the given segments to the origin.
    */
   void startKeysTransfer(IntSet segments, Address origin);

   /**
    * Confirm that this node has received cache topology with given topologyId and that it has
    * moved all the segments it does not own according to consistent hash
    * to the {@link ScatteredVersionManager.SegmentState#NOT_OWNED} state.
    */
   CompletionStage<Void> confirmRevokedSegments(int topologyId);
}
