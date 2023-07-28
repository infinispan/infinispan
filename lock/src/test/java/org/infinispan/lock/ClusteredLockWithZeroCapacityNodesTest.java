package org.infinispan.lock;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.jupiter.TestTags;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

@Tag(TestTags.SMOKE)
public class ClusteredLockWithZeroCapacityNodesTest extends ClusteredLockTest {

   static class NineLocks implements ParameterResolver {
      @Override
      public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
         return false;
      }

      @Override
      public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
         return null;
      }
   }
   @Override
   public Object[] factory() {
      return new Object[]{
            // REPL
            new ClusteredLockWithZeroCapacityNodesTest().numOwner(-1),
            // DIST
            new ClusteredLockWithZeroCapacityNodesTest().numOwner(1),
            new ClusteredLockWithZeroCapacityNodesTest().numOwner(9),
            };
   }

   protected int clusterSize() {
      return 3;
   }

   @Override
   protected GlobalConfigurationBuilder configure(int nodeId) {
      return super.configure(nodeId).zeroCapacityNode(nodeId % 2 == 1);
   }
}
