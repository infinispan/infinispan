package org.infinispan.notifications.cachelistener.event;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;

/**
 * An event passed in to methods annotated with {@link DataRehashed}.
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
    * @return The current consistent hash that was installed prior to the rehash.
    *         It is both for reading and writing before the rehash, and only for reading during the rehash.
    */
   ConsistentHash getConsistentHashAtStart();

   /**
    * @return The consistent hash that will be installed after the rehash.
    *         It will be used both for reading and writing once it is installed.
    */
   ConsistentHash getConsistentHashAtEnd();

   /**
    * @return The union of the current and future consistent hashes.
    *         It is used for writing during the rehash.
    */
   ConsistentHash getUnionConsistentHash();

   /**
    * @return Retrieves the new topology id after rehashing was triggered.
    */
   int getNewTopologyId();
}
