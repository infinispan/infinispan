package org.infinispan.rest

import java.io.IOException
import javax.ws.rs.container.{ContainerRequestFilter, ContainerResponseFilter}

import org.infinispan.commons.api.Lifecycle
import org.infinispan.manager.{DefaultCacheManager, EmbeddedCacheManager}
import org.infinispan.rest.configuration.{RestServerConfiguration, RestServerConfigurationBuilder}
import org.infinispan.rest.logging.{RestAccessLoggingHandler, Log}
import org.infinispan.server.core.CacheIgnoreAware
import org.jboss.resteasy.plugins.server.netty.NettyJaxrsServer
import org.jboss.resteasy.spi.ResteasyDeployment
import scala.collection.JavaConversions._

final class NettyRestServer (
      val cacheManager: EmbeddedCacheManager, val configuration: RestServerConfiguration,
      netty: NettyJaxrsServer, onStop: EmbeddedCacheManager => Unit) extends Lifecycle with Log with CacheIgnoreAware {

   override def start(): Unit = {
      netty.start()
      val deployment = netty.getDeployment
      configuration.getIgnoredCaches.foreach(ignoreCache)
      val restCacheManager = new RestCacheManager(cacheManager, isCacheIgnored)
      val server = new Server(configuration, restCacheManager)
      deployment.getRegistry.addSingletonResource(server)
      deployment.getProviderFactory.register(new RestAccessLoggingHandler, classOf[ContainerResponseFilter],
         classOf[ContainerRequestFilter])
      logStartRestServer(configuration.host(), configuration.port())
   }

   override def stop(): Unit = {
      netty.stop()
      onStop(cacheManager)
   }

}

object NettyRestServer extends Log {

   def apply(config: RestServerConfiguration): NettyRestServer = {
      NettyRestServer(config, new DefaultCacheManager(), cm => cm.stop())
   }

   def apply(config: RestServerConfiguration, cfgFile: String): NettyRestServer = {
      NettyRestServer(config, createCacheManager(cfgFile), cm => cm.stop())
   }

   def apply(config: RestServerConfiguration, cm: EmbeddedCacheManager): NettyRestServer = {
      NettyRestServer(config, cm, cm => ())
   }

   private def apply(config: RestServerConfiguration, cm: EmbeddedCacheManager,
         onStop: EmbeddedCacheManager => Unit): NettyRestServer = {
      // Start caches first, if not started
      startCaches(cm)

      val netty = new NettyJaxrsServer()
      val deployment = new ResteasyDeployment()
      netty.setDeployment(deployment)
      netty.setHostname(config.host())
      netty.setPort(config.port())
      netty.setRootResourcePath("")
      netty.setSecurityDomain(null)
      new NettyRestServer(cm, config, netty, onStop)
   }

   private def createCacheManager(cfgFile: String): EmbeddedCacheManager = {
      try {
         new DefaultCacheManager(cfgFile)
      } catch {
         case e: IOException =>
            logErrorReadingConfigurationFile(e, cfgFile)
            new DefaultCacheManager()
      }
   }

   private def startCaches(cm: EmbeddedCacheManager) = {
      // Start defined caches to avoid issues with lazily started caches
      import scala.collection.JavaConversions._
      cm.getCacheNames.foreach(x => SecurityActions.getCache(cm, x))

      // Finally, start default cache as well
      cm.getCache()
   }

}