package org.infinispan.distribution.ch;

import static org.testng.AssertJUnit.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.globalstate.impl.ScopedPersistentStateImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddressCache;
import org.infinispan.topology.PersistentUUID;
import org.testng.annotations.Test;

@Test
public abstract class CHPersistenceTest {

   protected abstract ConsistentHashFactory<?> createConsistentHashFactory();

   protected abstract ConsistentHash createConsistentHash();

   public void testCHPersistence() {
      ConsistentHash ch = createConsistentHash();
      generateRandomPersistentUUIDs(ch.getMembers());
      ScopedPersistentState state = new ScopedPersistentStateImpl("scope");
      ch.toScopedState(state);

      ConsistentHashFactory<?> hashFactory = createConsistentHashFactory();
      ConsistentHash restoredCH = hashFactory.fromPersistentState(state);
      assertEquals(ch, restoredCH);
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testCHPersistenceMissingMembers() {
      ConsistentHash ch = createConsistentHash();
      Map<Address, PersistentUUID> addressMap = generateRandomPersistentUUIDs(ch.getMembers());
      ScopedPersistentState state = new ScopedPersistentStateImpl("scope");
      ch.toScopedState(state);

      JGroupsAddressCache.flushAddressCaches();
      addressMap.remove(addressMap.keySet().iterator().next());
      addressMap.forEach((address, persistentUUID) -> {
         JGroupsAddressCache.putAddressPersistentUUID(address, persistentUUID);
      });

      ConsistentHashFactory<?> hashFactory = createConsistentHashFactory();
      ConsistentHash restoredCH = hashFactory.fromPersistentState(state);
      assertEquals(ch, restoredCH);
   }

   private Map<Address, PersistentUUID> generateRandomPersistentUUIDs(List<Address> members) {
      JGroupsAddressCache.flushAddressCaches();
      Map<Address, PersistentUUID> addressMap = new HashMap<>();
      for (Address member : members) {
         PersistentUUID uuid = PersistentUUID.randomUUID();
         JGroupsAddressCache.putAddressPersistentUUID(member, uuid);
         addressMap.put(member, uuid);
      }
      return addressMap;
   }
}
