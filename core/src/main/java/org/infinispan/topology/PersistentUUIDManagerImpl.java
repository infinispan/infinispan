package org.infinispan.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.UnaryOperator;

import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Implementation of the {@link PersistentUUIDManager} interface
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class PersistentUUIDManagerImpl implements PersistentUUIDManager {
   private static final Log log = LogFactory.getLog(PersistentUUIDManagerImpl.class);
   private final ConcurrentMap<Address, PersistentUUID> address2uuid = new ConcurrentHashMap<>();
   private final ConcurrentMap<PersistentUUID, Address> uuid2address = new ConcurrentHashMap<>();

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
      if (persistentUUID == null) {
         //A null would be invalid here, but letting it proceed would trigger an NPE
         //which would hide the real issue.
         return;
      }
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
