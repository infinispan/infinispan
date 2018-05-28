package org.infinispan.distribution.ch;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.globalstate.impl.ScopedPersistentStateImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.PersistentUUID;
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
      ch.remapAddresses(persistentUUIDManager.addressToPersistentUUID()).toScopedState(state);

      ConsistentHashFactory<?> hashFactory = createConsistentHashFactory();
      ConsistentHash restoredCH = hashFactory.fromPersistentState(state).remapAddresses(persistentUUIDManager.persistentUUIDToAddress());
      assertEquals(ch, restoredCH);
   }

   public void testCHPersistenceMissingMembers() {
      PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();
      ConsistentHash ch = createConsistentHash();
      Map<Address, PersistentUUID> addressMap = generateRandomPersistentUUIDs(ch.getMembers(), persistentUUIDManager);


      ScopedPersistentState state = new ScopedPersistentStateImpl("scope");
      ch.remapAddresses(persistentUUIDManager.addressToPersistentUUID()).toScopedState(state);

      persistentUUIDManager.removePersistentAddressMapping(addressMap.keySet().iterator().next());

      ConsistentHashFactory<?> hashFactory = createConsistentHashFactory();
      ConsistentHash restoredCH = hashFactory.fromPersistentState(state).remapAddresses(persistentUUIDManager.persistentUUIDToAddress());
      assertNull(restoredCH);
   }

   private Map<Address, PersistentUUID> generateRandomPersistentUUIDs(List<Address> members, PersistentUUIDManager persistentUUIDManager) {
      Map<Address, PersistentUUID> addressMap = new HashMap<>();
      for (Address member : members) {
         PersistentUUID uuid = PersistentUUID.randomUUID();
         persistentUUIDManager.addPersistentAddressMapping(member, uuid);
         addressMap.put(member, uuid);
      }
      return addressMap;
   }

}
