package org.infinispan.api;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Data inconsistency can happen in non-transactional caches. the tests replicates this scenario: assuming N1 and N2 are
 * owners of key K. N2 is the primary owner
 * <p/>
 * <ul>
 *    <li>N1 tries to update K. it forwards the command to N2.</li>
 *    <li>N2 acquires the lock, and forwards back to N1 (that applies the modification).</li>
 *    <li>N2 releases the lock and replies to N1.</li>
 *    <li>N1 applies again the modification without the lock.</li>
 * </ul>
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "api.NonDuplicateModificationTest")
public class NonDuplicateModificationTest extends MultipleCacheManagersTest {

   /**
    * ISPN-3354
    */
   public void testPut() throws Exception {
      performTestOn(Operation.PUT);
   }

   /**
    * ISPN-3354
    */
   public void testReplace() throws Exception {
      performTestOn(Operation.REPLACE);
   }

   /**
    * ISPN-3354
    */
   public void testRemove() throws Exception {
      performTestOn(Operation.REMOVE);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      builder.clustering().hash()
            .numSegments(60);
      createClusteredCaches(2, builder);
   }

   private void performTestOn(final Operation operation) throws Exception {
      final Object key = getKeyForCache(cache(0), cache(1));
      final ControlledRpcManager controlledRpcManager = replaceRpcManager(cache(1));

      cache(0).put(key, "v1");

      operation.setCommandToBlock(controlledRpcManager);

      assertKeyValue(key, "v1");

      Future<Void> future = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            operation.execute(cache(1), key, "v2");
            return null;
         }
      });

      controlledRpcManager.waitForCommandToBlock();

      cache(0).put(key, "v3");

      controlledRpcManager.stopBlocking();

      future.get();

      assertKeyValue(key, "v3");
   }

   private void assertKeyValue(Object key, Object expected) {
      for (Cache cache : caches()) {
         AssertJUnit.assertEquals("Wrong value for key " + key + " on " + address(cache), expected, cache.get(key));
      }
   }

   private ControlledRpcManager replaceRpcManager(Cache cache) {
      RpcManager rpcManager = TestingUtil.extractComponent(cache, RpcManager.class);
      ControlledRpcManager controlledRpcManager = new ControlledRpcManager(rpcManager);
      TestingUtil.replaceComponent(cache, RpcManager.class, controlledRpcManager, true);
      return controlledRpcManager;
   }

   private enum Operation {
      PUT(PutKeyValueCommand.class),
      REMOVE(RemoveCommand.class),
      REPLACE(ReplaceCommand.class);
      private final Class<?> classToBlock;

      Operation(Class<?> classToBlock) {
         this.classToBlock = classToBlock;
      }

      private void setCommandToBlock(ControlledRpcManager rpcManager) {
         rpcManager.blockAfter(classToBlock);
      }

      private void execute(Cache<Object, Object> cache, Object key, Object value) {
         switch (this) {
            case PUT:
               cache.put(key, value);
               break;
            case REMOVE:
               cache.remove(key);
               break;
            case REPLACE:
               cache.replace(key, value);
               break;
         }
      }
   }
}
