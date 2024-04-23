package org.infinispan.topology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.UnaryOperator;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.globalstate.GlobalStateProvider;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Implementation of the {@link PersistentUUIDManager} interface
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
@Scope(Scopes.GLOBAL)
public class PersistentUUIDManagerImpl implements PersistentUUIDManager, GlobalStateProvider {

   private static final String ADDRESS_STATE_PREFIX = "address.";
   private static final Log log = LogFactory.getLog(PersistentUUIDManagerImpl.class);
   private final ConcurrentMap<Address, PersistentUUID> address2uuid = new ConcurrentHashMap<>();
   private final ConcurrentMap<PersistentUUID, Address> uuid2address = new ConcurrentHashMap<>();

   @Inject @ComponentName(KnownComponentNames.INTERNAL_MARSHALLER)
   Marshaller marshaller;

   @Inject
   GlobalStateManager globalStateManager;

   @Start
   protected void preStart() {
      if (globalStateManager != null) globalStateManager.registerStateProvider(this);
   }

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
      return address2uuid::get;
   }

   @Override
   public UnaryOperator<Address> persistentUUIDToAddress() {
      return uuid2address::get;
   }

   @Override
   public UnaryOperator<Address> persistentUUIDToAddressConstrained(Set<Address> allowed) {
      return key -> {
         Address out = uuid2address.get(key);
         if (out == null || !allowed.contains(out))
            return null;

         return out;
      };
   }

   @Override
   public void prepareForPersist(ScopedPersistentState globalState) {
      uuid2address.forEach((uuid, addr) -> {
         String key = String.valueOf(uuid);
         String value = serialize(addr);
         globalState.setProperty(String.format("%s%s", ADDRESS_STATE_PREFIX, key), value);
      });
   }

   @Override
   public void prepareForRestore(ScopedPersistentState globalState) {
      Map<PersistentUUID, Address> mapping = new HashMap<>();
      globalState.forEach((k, v) -> {
         if (k.startsWith(ADDRESS_STATE_PREFIX)) {
            PersistentUUID uuid = PersistentUUID.fromString(k.replace(ADDRESS_STATE_PREFIX, ""));
            Address address = deserialize(v);

            if (mapping.put(uuid, address) != null)
               throw new IllegalStateException("Repeated mapping for: " + k);
         }
      });

      for (Map.Entry<PersistentUUID, Address> entry : mapping.entrySet()) {
         if (uuid2address.putIfAbsent(entry.getKey(), entry.getValue()) == null) {
            address2uuid.put(entry.getValue(), entry.getKey());
         }
      }
   }

   private String serialize(Address address) {
      try {
         byte[] b = marshaller.objectToByteBuffer(address);
         return Base64.getEncoder().encodeToString(b);
      } catch (IOException | InterruptedException e) {
         throw new RuntimeException("Failed serializing address", e);
      }
   }

   private Address deserialize(String value) {
      try {
         byte[] b = Base64.getDecoder().decode(value);
         return (Address) marshaller.objectFromByteBuffer(b);
      } catch (IOException | ClassNotFoundException e) {
         throw new RuntimeException("Failed reading value", e);
      }
   }
}
