package org.infinispan.topology;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;

import java.util.List;
import java.util.function.UnaryOperator;

/**
 * PersistentUUIDManager maintains a mapping of {@link PersistentUUID}s present in the cluster
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
@Scope(Scopes.GLOBAL)
public interface PersistentUUIDManager {
   /**
    * Adds a mapping between an {@link Address} and a {@link PersistentUUID}
    * @param address
    * @param persistentUUID
    */
   void addPersistentAddressMapping(Address address, PersistentUUID persistentUUID);

   /**
    * Retrieves the {@link PersistentUUID} of a node given its {@link Address}
    * @param address the address to lookup
    * @return the persistentuuid of the node, null if no mapping is present
    */
   PersistentUUID getPersistentUuid(Address address);

   /**
    * Retrieves the {@link Address} of a node given its {@link PersistentUUID}
    * @param persistentUUID the persistent uuid to lookup
    * @return the address of the node, null if no mapping is present
    */
   Address getAddress(PersistentUUID persistentUUID);

   /**
    * Removes any address mapping for the specified {@link PersistentUUID}
    * @param persistentUUID the {@link PersistentUUID} for which to remove mappings
    */
   void removePersistentAddressMapping(PersistentUUID persistentUUID);

   /**
    * Removes any address mapping for the specified {@link Address}
    * @param address the {@link Address} for which to remove mappings
    */
   void removePersistentAddressMapping(Address address);

   /**
    * Returns a list of {@link PersistentUUID}s for the supplied {@link Address}es
    * @param addresses
    * @return
    */
   List<PersistentUUID> mapAddresses(List<Address> addresses);

   /**
    * Provides a remapping operator which translates addresses to persistentuuids
    */
   UnaryOperator<Address> addressToPersistentUUID();

   /**
    * Provides a remapping operator which translates persistentuuids to addresses
    */
   UnaryOperator<Address> persistentUUIDToAddress();
}
