package org.infinispan.topology;

import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;

/**
 * PersistentUUIDManager maintains a mapping of {@link UUID}s present in the cluster
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
@Scope(Scopes.GLOBAL)
public interface PersistentUUIDManager {
   /**
    * Adds a mapping between an {@link Address} and a {@link UUID}
    * @param address
    * @param persistentUUID
    */
   void addPersistentAddressMapping(Address address, UUID persistentUUID);

   /**
    * Retrieves the {@link UUID} of a node given its {@link Address}
    * @param address the address to lookup
    * @return the persistentuuid of the node, null if no mapping is present
    */
   UUID getPersistentUuid(Address address);

   /**
    * Retrieves the {@link Address} of a node given its {@link UUID}
    * @param persistentUUID the persistent uuid to lookup
    * @return the address of the node, null if no mapping is present
    */
   Address getAddress(UUID persistentUUID);

   /**
    * Removes any address mapping for the specified {@link UUID}
    * @param persistentUUID the {@link UUID} for which to remove mappings
    */
   void removePersistentAddressMapping(UUID persistentUUID);

   /**
    * Removes any address mapping for the specified {@link Address}
    * @param address the {@link Address} for which to remove mappings
    */
   void removePersistentAddressMapping(Address address);

   /**
    * Returns a list of {@link UUID}s for the supplied {@link Address}es
    * @param addresses
    * @return a list of persistent UUIDs corresponding to the given addresses
    */
   List<UUID> mapAddresses(List<Address> addresses);

   /**
    * Provides a remapping operator which translates addresses to persistentuuids
    * @return a function that maps an address to its persistent UUID
    */
   Function<Address, UUID> addressToPersistentUUID();

   /**
    * Provides a remapping operator which translates persistentuuids to addresses
    * @return a function that maps a persistent UUID to its address
    */
   Function<UUID, Address> persistentUUIDToAddress();
}
