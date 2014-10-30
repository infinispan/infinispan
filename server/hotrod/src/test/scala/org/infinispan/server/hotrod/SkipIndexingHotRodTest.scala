package org.infinispan.server.hotrod

import org.infinispan.interceptors.base.BaseCustomInterceptor
import org.infinispan.context.{Flag, InvocationContext}
import org.infinispan.commands.{LocalFlagAffectedCommand, VisitableCommand}
import org.infinispan.commons.CacheException
import java.lang.reflect.Method
import test.HotRodTestingUtil._
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
      //PutRequest
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.put(k(m), 0, 0, v(m), 0), OperationStatus.Success)

      commandInterceptor.expectSkipIndexingFlag = true
      assertStatus(client.put(k(m), 0, 0, v(m), ProtocolFlag.SkipIndexing.id), OperationStatus.Success)
      assertStatus(client.put(k(m), 0, 0, v(m), join(ProtocolFlag.SkipIndexing.id,
                                                     ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.SuccessWithPrevious)
   }

   def testReplace(m: Method) {
      //ReplaceRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.replace(k(m), 0, 0, v(m), 0), OperationStatus.OperationNotExecuted)

      commandInterceptor.expectSkipIndexingFlag = true
      assertStatus(client.replace(k(m), 0, 0, v(m), ProtocolFlag.SkipIndexing.id), OperationStatus.OperationNotExecuted)
      assertStatus(client.replace(k(m), 0, 0, v(m), join(ProtocolFlag.SkipIndexing.id,
                                                         ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.OperationNotExecuted)

   }

   def testPutIfAbsent(m: Method) {
      //PutIfAbsentRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.putIfAbsent(k(m), 0, 0, v(m), 0), OperationStatus.Success)

      commandInterceptor.expectSkipIndexingFlag = true
      assertStatus(client.putIfAbsent(k(m), 0, 0, v(m), ProtocolFlag.SkipIndexing.id), OperationStatus.OperationNotExecuted)
      assertStatus(client.putIfAbsent(k(m), 0, 0, v(m), join(ProtocolFlag.SkipIndexing.id,
                                                             ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.NotExecutedWithPrevious)
   }

   def testReplaceIfUnmodified(m: Method) {
      //ReplaceIfUnmodifiedRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.replaceIfUnmodified(k(m), 0, 0, v(m), 0, 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipIndexingFlag = true
      assertStatus(client.replaceIfUnmodified(k(m), 0, 0, v(m), 0, ProtocolFlag.SkipIndexing.id), OperationStatus.KeyDoesNotExist)
      assertStatus(client.replaceIfUnmodified(k(m), 0, 0, v(m), 0, join(ProtocolFlag.SkipIndexing.id,
                                                                        ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.KeyDoesNotExist)
   }

   def testGet(m: Method) {
      //GetRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.get(k(m), 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.get(k(m), ProtocolFlag.SkipIndexing.id), OperationStatus.KeyDoesNotExist)
      assertStatus(client.get(k(m), join(ProtocolFlag.SkipIndexing.id,
                                         ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.KeyDoesNotExist)
   }

   def testGetWithVersion(m: Method) {
      //GetWithVersionRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.getWithVersion(k(m), 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.getWithVersion(k(m), ProtocolFlag.SkipIndexing.id), OperationStatus.KeyDoesNotExist)
      assertStatus(client.getWithVersion(k(m), join(ProtocolFlag.SkipIndexing.id,
                                                    ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.KeyDoesNotExist)

   }

   def testGetWithMetadata(m: Method) {
      //GetWithMetadataRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.getWithMetadata(k(m), 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.getWithMetadata(k(m), ProtocolFlag.SkipIndexing.id), OperationStatus.KeyDoesNotExist)
      assertStatus(client.getWithMetadata(k(m), join(ProtocolFlag.SkipIndexing.id,
                                                     ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.KeyDoesNotExist)
   }

   def testRemove(m: Method) {
      //RemoveRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.remove(k(m), 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipIndexingFlag = true
      assertStatus(client.remove(k(m), ProtocolFlag.SkipIndexing.id), OperationStatus.KeyDoesNotExist)
      assertStatus(client.remove(k(m), join(ProtocolFlag.SkipIndexing.id,
                                            ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.KeyDoesNotExist)
   }

   def testRemoveIfUnmodified(m: Method) {
      //RemoveIfUnmodifiedRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.removeIfUnmodified(k(m), 0, 0, v(m), 0, 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipIndexingFlag = true
      assertStatus(client.removeIfUnmodified(k(m), 0, 0, v(m), 0, ProtocolFlag.SkipIndexing.id), OperationStatus.KeyDoesNotExist)
      assertStatus(client.removeIfUnmodified(k(m), 0, 0, v(m), 0, join(ProtocolFlag.SkipIndexing.id,
                                                                       ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.KeyDoesNotExist)
   }

   def testContainsKey(m: Method) {
      //ContainsKeyRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.containsKey(k(m), 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipIndexingFlag = false
      assertStatus(client.containsKey(k(m), ProtocolFlag.SkipIndexing.id), OperationStatus.KeyDoesNotExist)
      assertStatus(client.containsKey(k(m), join(ProtocolFlag.SkipIndexing.id,
                                                 ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.KeyDoesNotExist)
   }

   private def init(): SkipIndexingFlagCheckCommandInterceptor = {
      val iterator = cacheManager.getCache(cacheName).getAdvancedCache.getInterceptorChain.iterator()
      while (iterator.hasNext) {
         val commandInterceptor = iterator.next()
         if (commandInterceptor.isInstanceOf[SkipIndexingFlagCheckCommandInterceptor])
            return commandInterceptor.asInstanceOf[SkipIndexingFlagCheckCommandInterceptor]
      }

      val ci = new SkipIndexingFlagCheckCommandInterceptor
      cacheManager.getCache(cacheName).getAdvancedCache.addInterceptor(ci, 1)
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
         case flagAffectedCommand: LocalFlagAffectedCommand =>
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
