package org.infinispan.topology;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Implementation of the {@link PersistentUUIDManager} interface
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class PersistentUUIDManagerImpl implements PersistentUUIDManager {
   private static final Log log = LogFactory.getLog(PersistentUUIDManagerImpl.class);
   private final EquivalentConcurrentHashMapV8<Address, PersistentUUID> address2uuid =
         new EquivalentConcurrentHashMapV8<>(AnyEquivalence.getInstance(), AnyEquivalence.getInstance());
   private final EquivalentConcurrentHashMapV8<PersistentUUID, Address> uuid2address =
         new EquivalentConcurrentHashMapV8<>(AnyEquivalence.getInstance(), AnyEquivalence.getInstance());

   @Override
   public void addPersistentAddressMapping(Address address, PersistentUUID persistentUUID) {
      address2uuid.put(address, persistentUUID);
      uuid2address.put(persistentUUID, address);
   }

   @Override
   public PersistentUUID getPersistentUuid(Address address) {
      return address2uuid.get(address);
   }

   @Override
   public Address getAddress(PersistentUUID persistentUUID) {
      return uuid2address.get(persistentUUID);
   }

   @Override
   public void removePersistentAddressMapping(PersistentUUID persistentUUID) {
      Address address = uuid2address.get(persistentUUID);
      if (address != null) {
         address2uuid.remove(address);
         uuid2address.remove(persistentUUID);
      }
   }

   @Override
   public void removePersistentAddressMapping(Address address) {
      PersistentUUID uuid = address2uuid.get(address);
      if (uuid != null) {
         uuid2address.remove(uuid);
         address2uuid.remove(address);
      }
   }

   @Override
   public List<PersistentUUID> mapAddresses(List<Address> addresses) {
      ArrayList<PersistentUUID> list = new ArrayList<>(addresses.size());
      for(Address address : addresses) {
         PersistentUUID persistentUUID = address2uuid.get(address);
         if (persistentUUID == null) {
            // This should never happen, but if it does, better log it here to avoid it being swallowed elsewhere
            NullPointerException npe = new NullPointerException();
            log.fatal("Cannot find mapping for address "+address, npe);
            throw npe;
         } else {
            list.add(persistentUUID);
         }
      }
      return list;
   }

   @Override
   public UnaryOperator<Address> addressToPersistentUUID() {
      return (address) -> address2uuid.get(address);
   }

   @Override
   public UnaryOperator<Address> persistentUUIDToAddress() {
      return (address) -> uuid2address.get(address);
   }
}
