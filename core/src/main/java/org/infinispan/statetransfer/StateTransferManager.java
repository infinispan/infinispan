package org.infinispan.statetransfer;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.configuration.cache.StateTransferConfiguration;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;

/**
 * A component that manages the state transfer when the topology of the cluster changes.
 *
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @author Mircea Markus
 * @author anistor@redhat.com
 * @since 5.1
 */
@Scope(Scopes.NAMED_CACHE)
public interface StateTransferManager {

   //todo [anistor] this is inaccurate. this node does not hold state yet in current implementation
   boolean isJoinComplete();

   /**
    * Checks if an inbound state transfer is in progress.
    */
   boolean isStateTransferInProgress();

   /**
    * Checks if an inbound state transfer is in progress for a given key.
    *
    * @deprecated since 10.0; to be removed in next major version
    */
   @Deprecated
   default boolean isStateTransferInProgressForKey(Object key) {
      return getStateConsumer().isStateTransferInProgressForKey(key);
   }

   void start() throws Exception;

   /**
    * Wait for the local cache to receive initial state from the other members.
    *
    * <p>Does nothing if {@link StateTransferConfiguration#awaitInitialTransfer()} is disabled.</p>
    */
   void waitForInitialStateTransferToComplete();

   void stop();

   /**
    * If there is an state transfer happening at the moment, this method forwards the supplied command to the nodes that
    * are new owners of the data, in order to assure consistency.
    *
    * @deprecated Since 14.0. To be removed without replacement.
    */
   @Deprecated
   default Map<Address, Response> forwardCommandIfNeeded(TopologyAffectedCommand command, Set<Object> affectedKeys, Address origin) {
      return Collections.emptyMap();
   }

   String getRebalancingStatus() throws Exception;

   StateConsumer getStateConsumer();

   StateProvider getStateProvider();
}
