package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;

import java.lang.reflect.Method;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.testng.annotations.Test;

/**
 * Tests if the {@link ProtocolFlag#SkipIndexing} flag is processed on server.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@Test(groups = "functional", testName = "server.hotrod.SkipIndexingHotRodTest")
public class SkipIndexingHotRodTest extends HotRodSingleNodeTest {

   private static final int SKIP_INDEXING_AND_RETURN_PREVIOUS_VALUE = join(ProtocolFlag.SkipIndexing.getValue(), ProtocolFlag.ForceReturnPreviousValue.getValue());

   public void testPut(Method m) {
      SkipIndexingFlagCheckCommandInterceptor commandInterceptor = init();
      //PUT
      commandInterceptor.expectSkipIndexingFlag = false;
      assertStatus(client().put(k(m), 0, 0, v(m), 0), OperationStatus.Success);

      commandInterceptor.expectSkipIndexingFlag = true;
      assertStatus(client().put(k(m), 0, 0, v(m), ProtocolFlag.SkipIndexing.getValue()), OperationStatus.Success);
      assertStatus(client().put(k(m), 0, 0, v(m), SKIP_INDEXING_AND_RETURN_PREVIOUS_VALUE),
                   OperationStatus.SuccessWithPrevious);
   }

   public void testReplace(Method m) {
      //REPLACE
      SkipIndexingFlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipIndexingFlag = false;
      assertStatus(client().replace(k(m), 0, 0, v(m), 0), OperationStatus.OperationNotExecuted);

      commandInterceptor.expectSkipIndexingFlag = true;
      assertStatus(client().replace(k(m), 0, 0, v(m), ProtocolFlag.SkipIndexing.getValue()),
                   OperationStatus.OperationNotExecuted);
      assertStatus(client().replace(k(m), 0, 0, v(m), SKIP_INDEXING_AND_RETURN_PREVIOUS_VALUE),
                   OperationStatus.OperationNotExecuted);

   }

   public void testPutIfAbsent(Method m) {
      //PUT_IF_ABSENT
      SkipIndexingFlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipIndexingFlag = false;
      assertStatus(client().putIfAbsent(k(m), 0, 0, v(m), 0), OperationStatus.Success);

      commandInterceptor.expectSkipIndexingFlag = true;
      assertStatus(client().putIfAbsent(k(m), 0, 0, v(m), ProtocolFlag.SkipIndexing.getValue()),
                   OperationStatus.OperationNotExecuted);
      assertStatus(client().putIfAbsent(k(m), 0, 0, v(m), SKIP_INDEXING_AND_RETURN_PREVIOUS_VALUE),
                   OperationStatus.NotExecutedWithPrevious);
   }

   public void testReplaceIfUnmodified(Method m) {
      //REPLACE_IF_UNMODIFIED
      SkipIndexingFlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipIndexingFlag = false;
      assertStatus(client().replaceIfUnmodified(k(m), 0, 0, v(m), 0, 0), OperationStatus.KeyDoesNotExist);

      commandInterceptor.expectSkipIndexingFlag = true;
      assertStatus(client().replaceIfUnmodified(k(m), 0, 0, v(m), 0, ProtocolFlag.SkipIndexing.getValue()),
                   OperationStatus.KeyDoesNotExist);
      assertStatus(client().replaceIfUnmodified(k(m), 0, 0, v(m), 0, SKIP_INDEXING_AND_RETURN_PREVIOUS_VALUE),
                   OperationStatus.KeyDoesNotExist);
   }

   public void testGet(Method m) {
      //GET
      SkipIndexingFlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipIndexingFlag = false;
      assertStatus(client().get(k(m), 0), OperationStatus.KeyDoesNotExist);

      commandInterceptor.expectSkipIndexingFlag = false;
      assertStatus(client().get(k(m), ProtocolFlag.SkipIndexing.getValue()), OperationStatus.KeyDoesNotExist);
      assertStatus(client().get(k(m), SKIP_INDEXING_AND_RETURN_PREVIOUS_VALUE),
                   OperationStatus.KeyDoesNotExist);
   }

   public void testGetWithVersion(Method m) {
      //GET_WITH_VERSION
      SkipIndexingFlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipIndexingFlag = false;
      assertStatus(client().getWithVersion(k(m), 0), OperationStatus.KeyDoesNotExist);

      commandInterceptor.expectSkipIndexingFlag = false;
      assertStatus(client().getWithVersion(k(m), ProtocolFlag.SkipIndexing.getValue()),
                   OperationStatus.KeyDoesNotExist);
      assertStatus(client().getWithVersion(k(m), SKIP_INDEXING_AND_RETURN_PREVIOUS_VALUE),
                   OperationStatus.KeyDoesNotExist);

   }

   public void testGetWithMetadata(Method m) {
      //GET_WITH_METADATA
      SkipIndexingFlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipIndexingFlag = false;
      assertStatus(client().getWithMetadata(k(m), 0), OperationStatus.KeyDoesNotExist);

      commandInterceptor.expectSkipIndexingFlag = false;
      assertStatus(client().getWithMetadata(k(m), ProtocolFlag.SkipIndexing.getValue()),
                   OperationStatus.KeyDoesNotExist);
      assertStatus(client().getWithMetadata(k(m), SKIP_INDEXING_AND_RETURN_PREVIOUS_VALUE),
                   OperationStatus.KeyDoesNotExist);
   }

   public void testRemove(Method m) {
      //REMOVE
      SkipIndexingFlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipIndexingFlag = false;
      assertStatus(client().remove(k(m), 0), OperationStatus.KeyDoesNotExist);

      commandInterceptor.expectSkipIndexingFlag = true;
      assertStatus(client().remove(k(m), ProtocolFlag.SkipIndexing.getValue()), OperationStatus.KeyDoesNotExist);
      assertStatus(client().remove(k(m), SKIP_INDEXING_AND_RETURN_PREVIOUS_VALUE), OperationStatus.KeyDoesNotExist);
   }

   public void testRemoveIfUnmodified(Method m) {
      //REMOVE_IF_UNMODIFIED
      SkipIndexingFlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipIndexingFlag = false;
      assertStatus(client().removeIfUnmodified(k(m), 0, 0), OperationStatus.KeyDoesNotExist);

      commandInterceptor.expectSkipIndexingFlag = true;
      assertStatus(client().removeIfUnmodified(k(m), 0, ProtocolFlag.SkipIndexing.getValue()),
                   OperationStatus.KeyDoesNotExist);
      assertStatus(client().removeIfUnmodified(k(m), 0, SKIP_INDEXING_AND_RETURN_PREVIOUS_VALUE),
                   OperationStatus.KeyDoesNotExist);
   }

   public void testContainsKey(Method m) {
      //CONTAINS_KEY
      SkipIndexingFlagCheckCommandInterceptor commandInterceptor = init();
      commandInterceptor.expectSkipIndexingFlag = false;
      assertStatus(client().containsKey(k(m), 0), OperationStatus.KeyDoesNotExist);

      commandInterceptor.expectSkipIndexingFlag = false;
      assertStatus(client().containsKey(k(m), ProtocolFlag.SkipIndexing.getValue()), OperationStatus.KeyDoesNotExist);
      assertStatus(client().containsKey(k(m), SKIP_INDEXING_AND_RETURN_PREVIOUS_VALUE),
                   OperationStatus.KeyDoesNotExist);
   }

   private SkipIndexingFlagCheckCommandInterceptor init() {
      AsyncInterceptorChain interceptorChain = extractInterceptorChain(cacheManager.getCache(cacheName));
      SkipIndexingFlagCheckCommandInterceptor interceptor =
         interceptorChain.findInterceptorExtending(SkipIndexingFlagCheckCommandInterceptor.class);
      if (interceptor != null)
         return interceptor;

      SkipIndexingFlagCheckCommandInterceptor ci = new SkipIndexingFlagCheckCommandInterceptor();
      interceptorChain.addInterceptor(ci, 1);
      return ci;
   }

   private static int join(int flagId, int joinId) {
      return joinId | flagId;
   }
}

class SkipIndexingFlagCheckCommandInterceptor extends BaseAsyncInterceptor {

   volatile boolean expectSkipIndexingFlag = false;

   @Override
   public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (command instanceof FlagAffectedCommand) {
         FlagAffectedCommand flagAffectedCommand = (FlagAffectedCommand) command;
         boolean hasFlag = flagAffectedCommand.hasAnyFlag(FlagBitSets.SKIP_INDEXING);
         if (expectSkipIndexingFlag && !hasFlag) {
            throw new CacheException("SKIP_INDEXING flag is expected!");
         } else if (!expectSkipIndexingFlag && hasFlag) {
            throw new CacheException("SKIP_INDEXING flag is *not* expected!");
         }
      }
      return invokeNext(ctx, command);
   }
}
