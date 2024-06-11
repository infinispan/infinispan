package org.infinispan.client.hotrod.retry;

import java.util.List;

import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.BaseControlledConsistentHashFactory;

/**
 * Consistent hash factory implementation keeping segments stable as nodes are stopped:
 *
 * <ol>
 * <li>0 -> A, 1 -> B, 2 -> C
 * <li>When A is stopped: 0, 1 -> B, 2 -> C
 * <li>When B is also stopped: 0, 1, 2 -> C
 * </ol>
 */
public class StableControlledConsistentHashFactory
      extends BaseControlledConsistentHashFactory<DefaultConsistentHash> {
   public StableControlledConsistentHashFactory() {
      super(new DefaultTrait(), 3);
   }

   @Override
   protected int[][] assignOwners(int numSegments, List<Address> members) {
      switch (members.size()) {
         case 1:
            return new int[][]{{0}, {0}, {0}};
         case 2:
            return new int[][]{{0}, {0}, {1}};
         default:
            return new int[][]{{0}, {1}, {2}};
      }
   }

   @AutoProtoSchemaBuilder(
         includeClasses = {StableControlledConsistentHashFactory.class},
         schemaFileName = "test.client.CompleteShutdownDistRetryTest.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.client.CompleteShutdownDistRetryTest"
   )
   public interface SCI extends SerializationContextInitializer {
   }
}
