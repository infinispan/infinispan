package org.infinispan.server.hotrod.event

import java.lang.reflect.Method
import java.util
import java.util.concurrent.{TimeUnit, ArrayBlockingQueue}
import org.infinispan.filter.Converter
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.metadata.Metadata
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.infinispan.server.hotrod.test._
import org.infinispan.server.hotrod.{Bytes, HotRodServer, HotRodSingleNodeTest}
import org.testng.annotations.Test

/**
 * @author Galder Zamarreño
 */
@Test(groups = Array("functional"), testName = "server.hotrod.event.HotRodCustomEventsTest")
class HotRodCustomEventsTest extends HotRodSingleNodeTest {

   val converterFactory = new TestConverterFactory

   override protected def createStartHotRodServer(cacheManager: EmbeddedCacheManager): HotRodServer = {
      val builder = new HotRodServerConfigurationBuilder
      // Storing unmarshalled byte arrays, so nullify default marshaller
      builder.converterFactory("test-converter-factory", converterFactory).marshaller(null)
      HotRodTestingUtil.startHotRodServer(cacheManager, builder)
   }

   def testCustomEvents(m: Method) {
      implicit val eventListener = new CustomEventListener
      converterFactory.dynamic = false
      withClientListener(converterFactory = Some(("test-converter-factory", List.empty))) { () =>
         expectNoEvents()
         val key = k(m)
         val keyLength = key.length.toByte
         val value = v(m)
         val valueLength = value.length.toByte
         client.put(key, 0, 0, value)
         expectSingleCustomEvent(Array(keyLength) ++ key ++ Array(valueLength) ++ value)
         val value2 = v(m, "v2-")
         val value2Length = value2.length.toByte
         client.put(key, 0, 0, value2)
         expectSingleCustomEvent(Array(keyLength) ++ key ++ Array(value2Length) ++ value2)
         client.remove(key)
         expectSingleCustomEvent(Array(keyLength) ++ key)
      }
   }

   def testParameterBasedConversion(m: Method) {
      implicit val eventListener = new CustomEventListener
      converterFactory.dynamic = true
      val customConvertKey = Array[Byte](4, 5, 6)
      val customConvertKeyLength = customConvertKey.length.toByte
      withClientListener(converterFactory = Some(("test-converter-factory", List(Array[Byte](4, 5, 6))))) { () =>
         expectNoEvents()
         val key = k(m)
         val keyLength = key.length.toByte
         val value = v(m)
         val valueLength = value.length.toByte
         client.put(key, 0, 0, value)
         expectSingleCustomEvent(Array(keyLength) ++ key ++ Array(valueLength) ++ value)
         val value2 = v(m, "v2-")
         val value2Length = value2.length.toByte
         client.put(key, 0, 0, value2)
         expectSingleCustomEvent(Array(keyLength) ++ key ++ Array(value2Length) ++ value2)
         client.remove(key)
         expectSingleCustomEvent(Array(keyLength) ++ key)
         client.put(customConvertKey, 0, 0, value)
         expectSingleCustomEvent(Array(customConvertKeyLength) ++ customConvertKey)
      }
   }

   def testConvertedEventsReplay(m: Method) {
      implicit val eventListener = new CustomEventListener
      converterFactory.dynamic = false
      val key = Array[Byte](1)
      val keyLength = key.length.toByte
      val value = Array[Byte](2)
      val valueLength = value.length.toByte
      client.put(key, 0, 0, value)
      withClientListener(converterFactory = Some(("test-converter-factory", List.empty))) { () =>
         expectSingleCustomEvent(Array(keyLength) ++ key ++ Array(valueLength) ++ value)
      }
   }

   private class CustomEventListener extends TestClientListener {
      private val customEvents =  new ArrayBlockingQueue[TestCustomEvent](128)

      override def customQueueSize(): Int = customEvents.size()
      override def pollCustom(): TestCustomEvent = customEvents.poll(10, TimeUnit.SECONDS)
      override def onCustom(event: TestCustomEvent): Unit = customEvents.add(event)
      override def getId: Bytes = Array[Byte](1, 2, 3)
   }

   class TestConverterFactory extends ConverterFactory {
      var dynamic = false
      override def getConverter[K, V, C](params: Array[AnyRef]): Converter[K, V, C] = {
         new Converter[Bytes, Bytes, Bytes] {
            override def convert(key: Bytes, value: Bytes, metadata: Metadata): Bytes = {
               val keyLength = key.length.toByte
               if (value == null || (dynamic && util.Arrays.equals(params.head.asInstanceOf[Bytes], key)))
                  Array(keyLength) ++ key
               else
                  Array(keyLength) ++ key ++ Array(value.length.toByte) ++ value
            }
         }.asInstanceOf[Converter[K, V, C]] // ugly but it works :|
      }
   }

}
