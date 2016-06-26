package org.infinispan.server.memcached

import java.util.function.Consumer

import org.testng.annotations.Test
import org.testng.Assert._
import org.infinispan.server.core.test.Stoppable
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder
import org.infinispan.configuration.cache.ConfigurationBuilder
import org.infinispan.commons.CacheConfigurationException
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.test.AbstractInfinispanTest

/**
 * Memcached server unit test.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.memcached.MemcachedServerTest")
class MemcachedServerTest extends AbstractInfinispanTest {

   def testValidateDefaultConfiguration {
      Stoppable.useCacheManager(TestCacheManagerFactory.createCacheManager(), new Consumer[EmbeddedCacheManager] {
         override def accept(cm: EmbeddedCacheManager): Unit = {
            Stoppable.useServer(new MemcachedServer(), new Consumer[MemcachedServer] {
               override def accept(server: MemcachedServer): Unit = {
                  server.start(new MemcachedServerConfigurationBuilder().build(), cm)
                  assertEquals(server.getHost, "127.0.0.1")
                  assertEquals(server.getPort, 11211)
               }
            })
         }
      })
   }

   @Test(expectedExceptions = Array(classOf[CacheConfigurationException]))
   def testValidateInvalidExpiration {
      val config = new ConfigurationBuilder
      config.expiration().lifespan(10)
      Stoppable.useCacheManager(TestCacheManagerFactory.createCacheManager(config), new Consumer[EmbeddedCacheManager] {
         override def accept(cm: EmbeddedCacheManager): Unit = {
            Stoppable.useServer(new MemcachedServer(), new Consumer[MemcachedServer] {
               override def accept(server: MemcachedServer): Unit = {
                  server.start(new MemcachedServerConfigurationBuilder().cache("memcachedCache").build(), cm)
                  fail("Server should not start when expiration is enabled")
               }
            })
         }
      })
   }

}