package org.infinispan.distribution.ch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.profiling.testinternals.Generator;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.ch.DefaultConsistentHashPersistenceTest")
public class DefaultConsistentHashPersistenceTest extends BaseCHPersistenceTest {

   @Override
   protected ConsistentHashFactory<?> createConsistentHashFactory() {
      return new DefaultConsistentHashFactory();
   }

   @Override
   public ConsistentHash createConsistentHash() {
      List<Address> members = new ArrayList<>();
      members.add(Generator.generateAddress());
      members.add(Generator.generateAddress());
      members.add(Generator.generateAddress());
      Map<Address, Float> capacityFactors = new HashMap<>();
      for (Address member : members) {
         capacityFactors.put(member, 1.0f);
      }
      DefaultConsistentHashFactory hashFactory = new DefaultConsistentHashFactory();
      return hashFactory.create(MurmurHash3.getInstance(), 2, 100, members, capacityFactors);
   }

}
