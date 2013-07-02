package org.infinispan.notifications.cachelistener.event;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;

/**
 * An event passed in to methods annotated with {@link DataRehashed}.
 *
 * <p>Note that the {@link #getConsistentHashAtStart()} and {@link #getConsistentHashAtEnd()}
 * may return different value in the "pre" event notification and in the "post" event notification.
 * For instance, the <em>end</em> CH in the "pre" notification may be a union of the <em>start</em> and
 * <em>end</em> CHs in the "post" notification.</p>
 *
 * <p>The result of the {@link #getNewTopologyId()} method is not guaranteed to be the same for the "pre"
 * and the "post" notification, either. However, the "post" value is guaranteed to be greater than or equal to
 * the "pre" value.</p>
 *
 * @author Manik Surtani
 * @author Dan Berindei
 * @since 5.0
 */
public interface DataRehashedEvent<K, V> extends Event<K, V> {

   /**
    * @return Retrieves the list of members before rehashing started.
    */
   Collection<Address> getMembersAtStart();

   /**
    * @return Retrieves the list of members after rehashing ended.
    */
   Collection<Address> getMembersAtEnd();

   /**
    * @return The unbalanced consistent hash before the rebalance started.
    */
   ConsistentHash getConsistentHashAtStart();

   /**
    * @return The consistent hash at the end of the rebalance.
    */
   ConsistentHash getConsistentHashAtEnd();

   /**
    * @return Retrieves the new topology id after rehashing was triggered.
    */
   int getNewTopologyId();
}
