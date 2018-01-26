package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;

import java.lang.reflect.Method;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.testng.annotations.Test;

/**
 * Tests if the {@link ProtocolFlag#SkipCacheLoader} flag is processed on server.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "server.hotrod.SkipCacheLoadHotRodTest")
public class SkipCacheLoadHotRodTest extends HotRodSingleNodeTest {
   private static final int SKIP_LOADER_AND_RETURN_PREVIOUS_VALUE = join(ProtocolFlag.SkipCacheLoader.getValue(),
                                                                         ProtocolFlag.ForceReturnPreviousValue
                                                                               .getValue());

   public void testPut(Method m) {
      FlagCheckCommandInterceptor commandInterceptor = init();
      //PUT
      commandInterceptor.expectSkipLoadFlag = false;
      assertStatus(client().put(k(m), 0, 0, v(m), 0), OperationStatus.Success);

      commandInterceptor.expectSkipLoadFlag = true;
      assertStatus(client().put(k(m), 0, 0, v(m), ProtocolFlag.SkipCacheLoader.getValue()), OperationStatus.Success);
      assertStatus(client().put(k(m), 0, 0, v(m), SKIP_LOADER_AND_RETURN_PREVIOUS_VALUE),
                   OperationStatus.SuccessWithPrevious);
   }

   public void testReplace(Method m) {
      //REPLACE
      FlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipLoadFlag = false;
      assertStatus(client().replace(k(m), 0, 0, v(m), 0), OperationStatus.OperationNotExecuted);

      commandInterceptor.expectSkipLoadFlag = false;
      assertStatus(client().replace(k(m), 0, 0, v(m), ProtocolFlag.SkipCacheLoader.getValue()),
                   OperationStatus.OperationNotExecuted);
      assertStatus(client().replace(k(m), 0, 0, v(m), SKIP_LOADER_AND_RETURN_PREVIOUS_VALUE),
                   OperationStatus.OperationNotExecuted);

   }

   public void testPutIfAbsent(Method m) {
      //PUT_IF_ABSENT
      FlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipLoadFlag = false;
      assertStatus(client().putIfAbsent(k(m), 0, 0, v(m), 0), OperationStatus.Success);

      commandInterceptor.expectSkipLoadFlag = false;
      assertStatus(client().putIfAbsent(k(m), 0, 0, v(m), ProtocolFlag.SkipCacheLoader.getValue()),
                   OperationStatus.OperationNotExecuted);
      assertStatus(client().putIfAbsent(k(m), 0, 0, v(m), SKIP_LOADER_AND_RETURN_PREVIOUS_VALUE),
                   OperationStatus.NotExecutedWithPrevious);
   }

   public void testReplaceIfUnmodified(Method m) {
      //REPLACE_IF_UNMODIFIED
      FlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipLoadFlag = false;
      assertStatus(client().replaceIfUnmodified(k(m), 0, 0, v(m), 0, 0), OperationStatus.KeyDoesNotExist);

      commandInterceptor.expectSkipLoadFlag = false;
      assertStatus(client().replaceIfUnmodified(k(m), 0, 0, v(m), 0, ProtocolFlag.SkipCacheLoader.getValue()),
                   OperationStatus.KeyDoesNotExist);
      assertStatus(client().replaceIfUnmodified(k(m), 0, 0, v(m), 0, SKIP_LOADER_AND_RETURN_PREVIOUS_VALUE),
                   OperationStatus.KeyDoesNotExist);
   }

   public void testGet(Method m) {
      //GET
      FlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipLoadFlag = false;
      assertStatus(client().get(k(m), 0), OperationStatus.KeyDoesNotExist);

      commandInterceptor.expectSkipLoadFlag = true;
      assertStatus(client().get(k(m), ProtocolFlag.SkipCacheLoader.getValue()), OperationStatus.KeyDoesNotExist);
      assertStatus(client().get(k(m), SKIP_LOADER_AND_RETURN_PREVIOUS_VALUE), OperationStatus.KeyDoesNotExist);
   }

   public void testGetWithVersion(Method m) {
      //GET_WITH_VERSION
      FlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipLoadFlag = false;
      assertStatus(client().getWithVersion(k(m), 0), OperationStatus.KeyDoesNotExist);

      commandInterceptor.expectSkipLoadFlag = true;
      assertStatus(client().getWithVersion(k(m), ProtocolFlag.SkipCacheLoader.getValue()),
                   OperationStatus.KeyDoesNotExist);
      assertStatus(client().getWithVersion(k(m), SKIP_LOADER_AND_RETURN_PREVIOUS_VALUE),
                   OperationStatus.KeyDoesNotExist);

   }

   public void testGetWithMetadata(Method m) {
      //GET_WITH_METADATA
      FlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipLoadFlag = false;
      assertStatus(client().getWithMetadata(k(m), 0), OperationStatus.KeyDoesNotExist);

      commandInterceptor.expectSkipLoadFlag = true;
      assertStatus(client().getWithMetadata(k(m), ProtocolFlag.SkipCacheLoader.getValue()),
                   OperationStatus.KeyDoesNotExist);
      assertStatus(client().getWithMetadata(k(m), SKIP_LOADER_AND_RETURN_PREVIOUS_VALUE),
                   OperationStatus.KeyDoesNotExist);
   }

   public void testRemove(Method m) {
      //REMOVE
      FlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipLoadFlag = false;
      assertStatus(client().remove(k(m), 0), OperationStatus.KeyDoesNotExist);

      commandInterceptor.expectSkipLoadFlag = true;
      assertStatus(client().remove(k(m), ProtocolFlag.SkipCacheLoader.getValue()), OperationStatus.KeyDoesNotExist);
      assertStatus(client().remove(k(m), SKIP_LOADER_AND_RETURN_PREVIOUS_VALUE), OperationStatus.KeyDoesNotExist);
   }

   public void testRemoveIfUnmodified(Method m) {
      //REMOVE_IF_UNMODIFIED
      FlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipLoadFlag = false;
      assertStatus(client().removeIfUnmodified(k(m), 0, 0), OperationStatus.KeyDoesNotExist);

      commandInterceptor.expectSkipLoadFlag = false;
      assertStatus(client().removeIfUnmodified(k(m), 0, ProtocolFlag.SkipCacheLoader.getValue()),
                   OperationStatus.KeyDoesNotExist);
      assertStatus(client().removeIfUnmodified(k(m), 0, SKIP_LOADER_AND_RETURN_PREVIOUS_VALUE),
                   OperationStatus.KeyDoesNotExist);
   }

   public void testContainsKey(Method m) {
      //CONTAINS_KEY
      FlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipLoadFlag = false;
      assertStatus(client().containsKey(k(m), 0), OperationStatus.KeyDoesNotExist);

      commandInterceptor.expectSkipLoadFlag = true;
      assertStatus(client().containsKey(k(m), ProtocolFlag.SkipCacheLoader.getValue()),
                   OperationStatus.KeyDoesNotExist);
      assertStatus(client().containsKey(k(m), SKIP_LOADER_AND_RETURN_PREVIOUS_VALUE), OperationStatus.KeyDoesNotExist);
   }

   private FlagCheckCommandInterceptor init() {
      AsyncInterceptorChain interceptorChain =
            cacheManager.getCache(cacheName).getAdvancedCache().getAsyncInterceptorChain();
      FlagCheckCommandInterceptor interceptor =
            interceptorChain.findInterceptorExtending(FlagCheckCommandInterceptor.class);
      if (interceptor != null)
         return interceptor;

      FlagCheckCommandInterceptor ci = new FlagCheckCommandInterceptor();
      interceptorChain.addInterceptor(ci, 1);
      return ci;
   }

   private static int join(int flagId, int joinId) {
      return joinId | flagId;
   }
}

class FlagCheckCommandInterceptor extends BaseCustomInterceptor {

   volatile boolean expectSkipLoadFlag = false;

   protected @Override
   Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (command instanceof FlagAffectedCommand) {
         FlagAffectedCommand flagAffectedCommand = (FlagAffectedCommand) command;
         if (flagAffectedCommand.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
            // this is the fast non-blocking read
            return super.handleDefault(ctx, command);
         }
         boolean hasFlag = flagAffectedCommand.hasAnyFlag(FlagBitSets.SKIP_CACHE_LOAD);
         if (expectSkipLoadFlag && !hasFlag) {
            throw new CacheException("SKIP_CACHE_LOAD flag is expected!");
         } else if (!expectSkipLoadFlag && hasFlag) {
            throw new CacheException("SKIP_CACHE_LOAD flag is *not* expected!");
         }
      }
      return super.handleDefault(ctx, command);
   }
}
