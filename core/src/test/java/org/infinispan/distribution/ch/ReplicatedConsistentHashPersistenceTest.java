package org.infinispan.distribution.ch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHashFactory;
import org.infinispan.profiling.testinternals.Generator;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.ch.ReplicatedConsistentHashPersistenceTest")
public class ReplicatedConsistentHashPersistenceTest extends BaseCHPersistenceTest {

   @Override
   protected ConsistentHashFactory<?> createConsistentHashFactory() {
      return new ReplicatedConsistentHashFactory();
   }

   @Override
   public ConsistentHash createConsistentHash() {
      List<Address> members = new ArrayList<>();
      members.add(Generator.generateAddress());
      members.add(Generator.generateAddress());
      members.add(Generator.generateAddress());
      ReplicatedConsistentHashFactory hashFactory = new ReplicatedConsistentHashFactory();
      return hashFactory.create(MurmurHash3.getInstance(), 2, 100, members, Collections.emptyMap());
   }

}
