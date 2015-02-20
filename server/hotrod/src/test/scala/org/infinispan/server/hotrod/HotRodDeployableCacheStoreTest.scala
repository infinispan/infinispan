package org.infinispan.server.hotrod

import java.lang.reflect.Method
import java.util.Properties

import org.infinispan.commons.configuration.ConfigurationFor
import org.infinispan.configuration.cache.{AsyncStoreConfiguration, SingletonStoreConfiguration, StoreConfiguration}
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.persistence.factory.DeployedCacheStoreFactory
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.mockito.Mockito
import org.testng.AssertJUnit._
import org.testng.annotations.Test

/**
 * Tests if adding Deplyable Cache Stores works properly in Hotrod server.
 *
 * <i>For more detailed tests refer to: {@link org.infinispan.persistence.factory.DeployedCacheStoreFactoryTest}</i>
 *
 * @author Sebastian Laskawiec
 * @since 7.2
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodDeployableCacheStoreTest")
class HotRodDeployableCacheStoreTest extends HotRodSingleNodeTest {
   override def createStartHotRodServer(cacheManager: EmbeddedCacheManager) = startHotRodServer(cacheManager, cacheName)

   @ConfigurationFor(classOf[Object])
   object configuration extends StoreConfiguration {

      override def async(): AsyncStoreConfiguration = ???

      override def fetchPersistentState(): Boolean = ???

      override def singletonStore(): SingletonStoreConfiguration = ???

      override def shared(): Boolean = ???

      override def properties(): Properties = ???

      override def purgeOnStartup(): Boolean = ???

      override def preload(): Boolean = ???

      override def ignoreModifications(): Boolean = ???
   }

   def testAddingNewDeployableCacheStoreToHotrodServer(m: Method) {
      // given
      val testedObject = new Object
      val deployedCacheStoreFactory = hotRodServer.getCacheRegistry(this.cacheName).getComponent(classOf[DeployedCacheStoreFactory])

      // when
      hotRodServer.addCacheStore(testedObject.getClass.getName, testedObject)
      val createdInstance = deployedCacheStoreFactory.createInstance(configuration)

      //then
      assertEquals(testedObject, createdInstance)
   }
}
