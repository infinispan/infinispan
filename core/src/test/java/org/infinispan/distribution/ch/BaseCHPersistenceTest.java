package org.infinispan.distribution.ch;

import static org.testng.AssertJUnit.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.infinispan.distribution.ch.impl.ConsistentHashFactory;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.globalstate.impl.ScopedPersistentStateImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.PersistentUUIDManager;
import org.infinispan.topology.PersistentUUIDManagerImpl;
import org.testng.annotations.Test;

@Test(groups = "unit")
public abstract class BaseCHPersistenceTest {

   protected abstract ConsistentHashFactory<?> createConsistentHashFactory();

   protected abstract ConsistentHash createConsistentHash();

   public void testCHPersistence() {
      PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();
      ConsistentHash ch = createConsistentHash();
      generateRandomPersistentUUIDs(ch.getMembers(), persistentUUIDManager);
      ScopedPersistentState state = new ScopedPersistentStateImpl("scope");
      ch.toScopedState(state, persistentUUIDManager.addressToPersistentUUID());

      var hashFactory = createConsistentHashFactory();
      ConsistentHash restoredCH = hashFactory.fromPersistentState(state, persistentUUIDManager.persistentUUIDToAddress()).consistentHash();
      assertEquals(ch, restoredCH);
   }

   public void testCHPersistenceMissingMembers() {
      PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();
      ConsistentHash ch = createConsistentHash();
      var addressMap = generateRandomPersistentUUIDs(ch.getMembers(), persistentUUIDManager);


      ScopedPersistentState state = new ScopedPersistentStateImpl("scope");
      ch.toScopedState(state, persistentUUIDManager.addressToPersistentUUID());

      var toRemove = addressMap.keySet().iterator().next();
      persistentUUIDManager.removePersistentAddressMapping(toRemove);

      var hashFactory = createConsistentHashFactory();
      var restoredCH = hashFactory.fromPersistentState(state, persistentUUIDManager.persistentUUIDToAddress());
      assertEquals(1, restoredCH.missingUuids().size());
      assertEquals(addressMap.get(toRemove), restoredCH.missingUuids().iterator().next());
   }

   private Map<Address, UUID> generateRandomPersistentUUIDs(List<Address> members, PersistentUUIDManager persistentUUIDManager) {
      Map<Address, UUID> addressMap = new HashMap<>();
      for (Address member : members) {
         var uuid = UUID.randomUUID();
         persistentUUIDManager.addPersistentAddressMapping(member, uuid);
         addressMap.put(member, uuid);
      }
      return addressMap;
   }

}
