package org.infinispan.server.core

import org.infinispan.marshall.VersionAwareMarshaller
import org.testng.annotations.{AfterClass, BeforeTest}
import org.infinispan.commands.RemoteCommandsFactory
import java.util.Random
import java.io.{ObjectOutputStream, ByteArrayOutputStream}
import org.infinispan.config.GlobalConfiguration

/**
 * Abstract class to help marshalling tests in different server modules.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class AbstractMarshallingTest {

   val marshaller = new VersionAwareMarshaller

   @BeforeTest
   def setUp {
      // Manual addition of externalizers to replication what happens in fully functional tests
      val globalCfg = new GlobalConfiguration
      new LifecycleCallbacks().addExternalizer(globalCfg)
      marshaller.inject(Thread.currentThread.getContextClassLoader, new RemoteCommandsFactory, globalCfg)
      marshaller.start
   }

   @AfterClass
   def tearDown = marshaller.stop

   protected def getBigByteArray: Array[Byte] = {
      val value = new String(randomByteArray(1000))
      val result = new ByteArrayOutputStream(1000)
      val oos = new ObjectOutputStream(result)
      oos.writeObject(value)
      result.toByteArray
   }

   private def randomByteArray(i: Int): Array[Byte] = {
      val r = new Random
      val result = new Array[Byte](i)
      r.nextBytes(result)
      result
   }

}