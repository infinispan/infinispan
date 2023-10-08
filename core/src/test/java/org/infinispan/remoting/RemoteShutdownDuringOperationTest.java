package org.infinispan.remoting;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.assertj.core.api.SoftAssertions;
import org.infinispan.cache.impl.EncoderCache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.expiration.impl.TouchCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.CheckPoint;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "remoting.RemoteShutdownDuringOperationTest")
public class RemoteShutdownDuringOperationTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "distSync";

   private TolerateSuspectOperation operation;

   protected RemoteShutdownDuringOperationTest withOperation(TolerateSuspectOperation operation) {
      this.operation = operation;
      return this;
   }


   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      int numOwners = 2;

      // Necessary to go to the remote node.
      if (operation == TolerateSuspectOperation.GET) numOwners = 1;

      cb.clustering().hash().numOwners(numOwners);
      createClusteredCaches(2, CACHE_NAME, TestDataSCI.INSTANCE, cb);
   }

   @Test
   public void testShutdownDuringOperation() throws Exception {
      EncoderCache<MagicKey, String> c0 = cache(0);
      EncoderCache<MagicKey, String> c1 = cache(1);

      assertThat(ComponentRegistry.of(c1).getStatus().isTerminated()).isFalse();

      MagicKey key = new MagicKey("remote-key", c1);

      assertThat(c1.put(key, "value")).isNull();
      assertThat(c0.get(key)).isEqualTo("value");

      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);
      Mocks.blockInboundCacheRpcCommand(c1, checkPoint, operation.verify());

      SoftAssertions softly = new SoftAssertions();
      CompletableFuture<Void> cs = operation.operate(c0, key, softly).toCompletableFuture();

      // Wait for the remote node to receive the command and shutdown it.
      checkPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS);
      c1.stop();
      assertThat(ComponentRegistry.of(c1).getStatus().isTerminated()).isTrue();

      // Proceed handling the command, which will fail with a CacheNotFoundResponse.
      checkPoint.trigger(Mocks.BEFORE_RELEASE, 1);

      // Wait for the command to complete.
      cs.get(10, TimeUnit.SECONDS);
      softly.assertAll();
   }

   @Override
   protected EncoderCache<MagicKey, String> cache(int index) {
      return (EncoderCache<MagicKey, String>) (Object) cache(index, CACHE_NAME);
   }

   @Override
   public Object[] factory() {
      return Stream.of(TolerateSuspectOperation.values())
            .map(op -> new RemoteShutdownDuringOperationTest().withOperation(op))
            .toArray();
   }

   @Override
   protected String parameters() {
      return "[operation=" + operation + "]";
   }

   private enum TolerateSuspectOperation {
      TOUCH {
         @Override
         public CompletionStage<Void> operate(EncoderCache<MagicKey, String> cache, MagicKey key, SoftAssertions softly) {
            return cache.touch(key, false)
                  .handle((touched, t) -> {
                     if (t != null) {
                        softly.fail("Unexpected exception", t);
                     } else {
                        softly.assertThat(touched).isTrue();
                     }
                     return null;
                  });
         }

         @Override
         public boolean verifyCommand(CacheRpcCommand cmd) {
            if (cmd instanceof SingleRpcCommand) {
               return ((SingleRpcCommand) cmd).getCommand() instanceof TouchCommand;
            }
            return false;
         }
      },
      GET {
         @Override
         public CompletionStage<Void> operate(EncoderCache<MagicKey, String> cache, MagicKey key, SoftAssertions softly) {
            return cache.getAsync(key)
                  .handle((value, t) -> {
                     if (t != null) {
                        softly.fail("Unexpected exception", t);
                     } else {
                        // Get operation has a single owner. Since it is shutdown, the return is null.
                        softly.assertThat(value).isNull();
                     }
                     return null;
                  });
         }

         @Override
         public boolean verifyCommand(CacheRpcCommand cmd) {
            return cmd instanceof ClusteredGetCommand;
         }
      };

      public abstract CompletionStage<Void> operate(EncoderCache<MagicKey, String> cache, MagicKey key, SoftAssertions softly);

      public abstract boolean verifyCommand(CacheRpcCommand cmd);

      public Predicate<? super CacheRpcCommand> verify() {
         return this::verifyCommand;
      }
   }
}
