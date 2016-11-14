package org.infinispan.server.hotrod

import java.lang.reflect.Method

import org.infinispan.commands.{FlagAffectedCommand, VisitableCommand}
import org.infinispan.commons.CacheException
import org.infinispan.context.{Flag, InvocationContext}
import org.infinispan.interceptors.base.BaseCustomInterceptor
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.testng.annotations.Test

/**
 * Tests if the {@link ProtocolFlag#SKIP_INDEXING} flag is processed on server.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@Test(groups = Array("functional"), testName = "server.hotrod.SkipIndexingHotRodTest")
class SkipIndexingHotRodTest extends HotRodSingleNodeTest {

   def testPut(m: Method) {
      val commandInterceptor = init()
      //PUT
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.put(k(m), 0, 0, v(m), 0), OperationStatus.Success)

      commandInterceptor.expectSkipIndexingFlag = true
      assertStatus(client.put(k(m), 0, 0, v(m), ProtocolFlag.SkipIndexing.getValue()), OperationStatus.Success)
      assertStatus(client.put(k(m), 0, 0, v(m), join(ProtocolFlag.SkipIndexing.getValue(),
                                                     ProtocolFlag.ForceReturnPreviousValue.getValue())), OperationStatus.SuccessWithPrevious)
   }

   def testReplace(m: Method) {
      //REPLACE
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.replace(k(m), 0, 0, v(m), 0), OperationStatus.OperationNotExecuted)

      commandInterceptor.expectSkipIndexingFlag = true
      assertStatus(client.replace(k(m), 0, 0, v(m), ProtocolFlag.SkipIndexing.getValue()), OperationStatus.OperationNotExecuted)
      assertStatus(client.replace(k(m), 0, 0, v(m), join(ProtocolFlag.SkipIndexing.getValue(),
                                                         ProtocolFlag.ForceReturnPreviousValue.getValue())), OperationStatus.OperationNotExecuted)

   }

   def testPutIfAbsent(m: Method) {
      //PUT_IF_ABSENT
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.putIfAbsent(k(m), 0, 0, v(m), 0), OperationStatus.Success)

      commandInterceptor.expectSkipIndexingFlag = true
      assertStatus(client.putIfAbsent(k(m), 0, 0, v(m), ProtocolFlag.SkipIndexing.getValue()), OperationStatus.OperationNotExecuted)
      assertStatus(client.putIfAbsent(k(m), 0, 0, v(m), join(ProtocolFlag.SkipIndexing.getValue(),
                                                             ProtocolFlag.ForceReturnPreviousValue.getValue())), OperationStatus.NotExecutedWithPrevious)
   }

   def testReplaceIfUnmodified(m: Method) {
      //REPLACE_IF_UNMODIFIED
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.replaceIfUnmodified(k(m), 0, 0, v(m), 0, 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipIndexingFlag = true
      assertStatus(client.replaceIfUnmodified(k(m), 0, 0, v(m), 0, ProtocolFlag.SkipIndexing.getValue()), OperationStatus.KeyDoesNotExist)
      assertStatus(client.replaceIfUnmodified(k(m), 0, 0, v(m), 0, join(ProtocolFlag.SkipIndexing.getValue(),
                                                                        ProtocolFlag.ForceReturnPreviousValue.getValue())), OperationStatus.KeyDoesNotExist)
   }

   def testGet(m: Method) {
      //GET
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.get(k(m), 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.get(k(m), ProtocolFlag.SkipIndexing.getValue()), OperationStatus.KeyDoesNotExist)
      assertStatus(client.get(k(m), join(ProtocolFlag.SkipIndexing.getValue(),
                                         ProtocolFlag.ForceReturnPreviousValue.getValue())), OperationStatus.KeyDoesNotExist)
   }

   def testGetWithVersion(m: Method) {
      //GET_WITH_VERSION
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.getWithVersion(k(m), 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.getWithVersion(k(m), ProtocolFlag.SkipIndexing.getValue()), OperationStatus.KeyDoesNotExist)
      assertStatus(client.getWithVersion(k(m), join(ProtocolFlag.SkipIndexing.getValue(),
                                                    ProtocolFlag.ForceReturnPreviousValue.getValue())), OperationStatus.KeyDoesNotExist)

   }

   def testGetWithMetadata(m: Method) {
      //GET_WITH_METADATA
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.getWithMetadata(k(m), 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.getWithMetadata(k(m), ProtocolFlag.SkipIndexing.getValue()), OperationStatus.KeyDoesNotExist)
      assertStatus(client.getWithMetadata(k(m), join(ProtocolFlag.SkipIndexing.getValue(),
                                                     ProtocolFlag.ForceReturnPreviousValue.getValue())), OperationStatus.KeyDoesNotExist)
   }

   def testRemove(m: Method) {
      //REMOVE
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.remove(k(m), 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipIndexingFlag = true
      assertStatus(client.remove(k(m), ProtocolFlag.SkipIndexing.getValue()), OperationStatus.KeyDoesNotExist)
      assertStatus(client.remove(k(m), join(ProtocolFlag.SkipIndexing.getValue(),
                                            ProtocolFlag.ForceReturnPreviousValue.getValue())), OperationStatus.KeyDoesNotExist)
   }

   def testRemoveIfUnmodified(m: Method) {
      //REMOVE_IF_UNMODIFIED
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.removeIfUnmodified(k(m), 0, 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipIndexingFlag = true
      assertStatus(client.removeIfUnmodified(k(m), 0, ProtocolFlag.SkipIndexing.getValue()), OperationStatus.KeyDoesNotExist)
      assertStatus(client.removeIfUnmodified(k(m), 0, join(ProtocolFlag.SkipIndexing.getValue(),
                                                                       ProtocolFlag.ForceReturnPreviousValue.getValue())), OperationStatus.KeyDoesNotExist)
   }

   def testContainsKey(m: Method) {
      //CONTAINS_KEY
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.containsKey(k(m), 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.containsKey(k(m), ProtocolFlag.SkipIndexing.getValue()), OperationStatus.KeyDoesNotExist)
      assertStatus(client.containsKey(k(m), join(ProtocolFlag.SkipIndexing.getValue(),
                                                 ProtocolFlag.ForceReturnPreviousValue.getValue())), OperationStatus.KeyDoesNotExist)
   }

   private def init(): SkipIndexingFlagCheckCommandInterceptor = {
      val iterator = cacheManager.getCache(cacheName).getAdvancedCache.getInterceptorChain.iterator()
      while (iterator.hasNext) {
         val commandInterceptor = iterator.next()
         if (commandInterceptor.isInstanceOf[SkipIndexingFlagCheckCommandInterceptor])
            return commandInterceptor.asInstanceOf[SkipIndexingFlagCheckCommandInterceptor]
      }

      val ci = new SkipIndexingFlagCheckCommandInterceptor
      cacheManager.getCache(cacheName).getAdvancedCache.getAsyncInterceptorChain().addInterceptor(ci, 1)
      ci
   }

   private def join(flagId: Int, joinId: Int): Int = {
      joinId | flagId
   }
}

class SkipIndexingFlagCheckCommandInterceptor extends BaseCustomInterceptor {

   @volatile var expectSkipIndexingFlag = false

   protected override def handleDefault(ctx: InvocationContext, command: VisitableCommand): AnyRef = {
      command match {
         case flagAffectedCommand: FlagAffectedCommand =>
            val hasFlag = flagAffectedCommand.hasFlag(Flag.SKIP_INDEXING)
            if (expectSkipIndexingFlag && !hasFlag) {
               throw new CacheException("SKIP_INDEXING flag is expected!")
            } else if (!expectSkipIndexingFlag && hasFlag) {
               throw new CacheException("SKIP_INDEXING flag is *not* expected!")
            }
         case _ =>
      }
      super.handleDefault(ctx, command)
   }
}
