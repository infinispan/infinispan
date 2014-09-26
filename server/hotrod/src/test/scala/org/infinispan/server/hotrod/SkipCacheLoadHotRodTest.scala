package org.infinispan.server.hotrod

import org.infinispan.interceptors.base.BaseCustomInterceptor
import org.infinispan.context.{Flag, InvocationContext}
import org.infinispan.commands.{LocalFlagAffectedCommand, VisitableCommand}
import org.infinispan.commons.CacheException
import java.lang.reflect.Method
import test.HotRodTestingUtil._
import org.testng.annotations.Test

/**
 * Tests if the {@link ProtocolFlag#SKIP_CACHE_LOAD} flag is processed on server.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = Array("functional"), testName = "server.hotrod.SkipCacheLoadHotRodTest")
class SkipCacheLoadHotRodTest extends HotRodSingleNodeTest {


   def testPut(m: Method) {
      val commandInterceptor = init()
      //PutRequest
      commandInterceptor.expectSkipLoadFlag = false
      assertStatus(client.put(k(m), 0, 0, v(m), 0), OperationStatus.Success)

      commandInterceptor.expectSkipLoadFlag = true
      assertStatus(client.put(k(m), 0, 0, v(m), ProtocolFlag.SkipCacheLoader.id), OperationStatus.Success)
      assertStatus(client.put(k(m), 0, 0, v(m), join(ProtocolFlag.SkipCacheLoader.id,
                                                     ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.SuccessWithPrevious)
   }

   def testReplace(m: Method) {
      //ReplaceRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipLoadFlag = false
      assertStatus(client.replace(k(m), 0, 0, v(m), 0), OperationStatus.OperationNotExecuted)

      commandInterceptor.expectSkipLoadFlag = false
      assertStatus(client.replace(k(m), 0, 0, v(m), ProtocolFlag.SkipCacheLoader.id), OperationStatus.OperationNotExecuted)
      assertStatus(client.replace(k(m), 0, 0, v(m), join(ProtocolFlag.SkipCacheLoader.id,
                                                         ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.OperationNotExecuted)

   }

   def testPutIfAbsent(m: Method) {
      //PutIfAbsentRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipLoadFlag = false
      assertStatus(client.putIfAbsent(k(m), 0, 0, v(m), 0), OperationStatus.Success)

      commandInterceptor.expectSkipLoadFlag = false
      assertStatus(client.putIfAbsent(k(m), 0, 0, v(m), ProtocolFlag.SkipCacheLoader.id), OperationStatus.OperationNotExecuted)
      assertStatus(client.putIfAbsent(k(m), 0, 0, v(m), join(ProtocolFlag.SkipCacheLoader.id,
                                                             ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.NotExecutedWithPrevious)
   }

   def testReplaceIfUnmodified(m: Method) {
      //ReplaceIfUnmodifiedRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipLoadFlag = false
      assertStatus(client.replaceIfUnmodified(k(m), 0, 0, v(m), 0, 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipLoadFlag = false
      assertStatus(client.replaceIfUnmodified(k(m), 0, 0, v(m), 0, ProtocolFlag.SkipCacheLoader.id), OperationStatus.KeyDoesNotExist)
      assertStatus(client.replaceIfUnmodified(k(m), 0, 0, v(m), 0, join(ProtocolFlag.SkipCacheLoader.id,
                                                                        ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.KeyDoesNotExist)
   }

   def testGet(m: Method) {
      //GetRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipLoadFlag = false
      assertStatus(client.get(k(m), 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipLoadFlag = true
      assertStatus(client.get(k(m), ProtocolFlag.SkipCacheLoader.id), OperationStatus.KeyDoesNotExist)
      assertStatus(client.get(k(m), join(ProtocolFlag.SkipCacheLoader.id,
                                         ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.KeyDoesNotExist)
   }

   def testGetWithVersion(m: Method) {
      //GetWithVersionRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipLoadFlag = false
      assertStatus(client.getWithVersion(k(m), 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipLoadFlag = true
      assertStatus(client.getWithVersion(k(m), ProtocolFlag.SkipCacheLoader.id), OperationStatus.KeyDoesNotExist)
      assertStatus(client.getWithVersion(k(m), join(ProtocolFlag.SkipCacheLoader.id,
                                                    ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.KeyDoesNotExist)

   }

   def testGetWithMetadata(m: Method) {
      //GetWithMetadataRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipLoadFlag = false
      assertStatus(client.getWithMetadata(k(m), 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipLoadFlag = true
      assertStatus(client.getWithMetadata(k(m), ProtocolFlag.SkipCacheLoader.id), OperationStatus.KeyDoesNotExist)
      assertStatus(client.getWithMetadata(k(m), join(ProtocolFlag.SkipCacheLoader.id,
                                                     ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.KeyDoesNotExist)
   }

   def testRemove(m: Method) {
      //RemoveRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipLoadFlag = false
      assertStatus(client.remove(k(m), 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipLoadFlag = true
      assertStatus(client.remove(k(m), ProtocolFlag.SkipCacheLoader.id), OperationStatus.KeyDoesNotExist)
      assertStatus(client.remove(k(m), join(ProtocolFlag.SkipCacheLoader.id,
                                            ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.KeyDoesNotExist)
   }

   def testRemoveIfUnmodified(m: Method) {
      //RemoveIfUnmodifiedRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipLoadFlag = false
      assertStatus(client.removeIfUnmodified(k(m), 0, 0, v(m), 0, 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipLoadFlag = false
      assertStatus(client.removeIfUnmodified(k(m), 0, 0, v(m), 0, ProtocolFlag.SkipCacheLoader.id), OperationStatus.KeyDoesNotExist)
      assertStatus(client.removeIfUnmodified(k(m), 0, 0, v(m), 0, join(ProtocolFlag.SkipCacheLoader.id,
                                                                       ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.KeyDoesNotExist)
   }

   def testContainsKey(m: Method) {
      //ContainsKeyRequest
      val commandInterceptor = init()
      commandInterceptor.expectSkipLoadFlag = false
      assertStatus(client.containsKey(k(m), 0), OperationStatus.KeyDoesNotExist)

      commandInterceptor.expectSkipLoadFlag = true
      assertStatus(client.containsKey(k(m), ProtocolFlag.SkipCacheLoader.id), OperationStatus.KeyDoesNotExist)
      assertStatus(client.containsKey(k(m), join(ProtocolFlag.SkipCacheLoader.id,
                                                 ProtocolFlag.ForceReturnPreviousValue.id)), OperationStatus.KeyDoesNotExist)
   }

   private def init(): FlagCheckCommandInterceptor = {
      val iterator = cacheManager.getCache(cacheName).getAdvancedCache.getInterceptorChain.iterator()
      while (iterator.hasNext) {
         val commandInterceptor = iterator.next()
         if (commandInterceptor.isInstanceOf[FlagCheckCommandInterceptor])
            return commandInterceptor.asInstanceOf[FlagCheckCommandInterceptor]
      }

      val ci = new FlagCheckCommandInterceptor
      cacheManager.getCache(cacheName).getAdvancedCache.addInterceptor(ci, 1)
      ci
   }

   private def join(flagId: Int, joinId: Int): Int = {
      joinId | flagId
   }
}

class FlagCheckCommandInterceptor extends BaseCustomInterceptor {

   @volatile var expectSkipLoadFlag = false

   protected override def handleDefault(ctx: InvocationContext, command: VisitableCommand): AnyRef = {
      command match {
         case flagAffectedCommand: LocalFlagAffectedCommand =>
            val hasFlag = flagAffectedCommand.hasFlag(Flag.SKIP_CACHE_LOAD)
            if (expectSkipLoadFlag && !hasFlag) {
               throw new CacheException("SKIP_CACHE_LOAD flag is expected!")
            } else if (!expectSkipLoadFlag && hasFlag) {
               throw new CacheException("SKIP_CACHE_LOAD flag is *not* expected!")
            }
         case _ =>
      }
      super.handleDefault(ctx, command)
   }
}
