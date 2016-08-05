package org.infinispan.scattered;

import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateProvider;

import java.util.Map;
import java.util.Set;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public interface ScatteredStateProvider extends StateProvider {

   Map<Integer, Long> getMaxVersions(Set<Integer> segments, int topologyId, Address origin);

   void startKeysTransfer(Set<Integer> segments, Address origin);
}
