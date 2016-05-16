package org.infinispan.server.hotrod

import java.util.concurrent.ThreadPoolExecutor.AbortPolicy
import java.util.concurrent._
import java.util.function.Predicate
import java.util.{EnumSet, ServiceLoader}
import javax.security.sasl.SaslServerFactory

import io.netty.channel.{Channel, ChannelInitializer}
import io.netty.util.concurrent.DefaultThreadFactory
import org.infinispan
import org.infinispan.commons.equivalence.AnyEquivalence
import org.infinispan.commons.marshall.Marshaller
import org.infinispan.commons.util.{CollectionFactory, ServiceFinder}
import org.infinispan.configuration.cache.{CacheMode, Configuration, ConfigurationBuilder}
import org.infinispan.context.Flag
import org.infinispan.distexec._
import org.infinispan.eviction.EvictionStrategy
import org.infinispan.factories.ComponentRegistry
import org.infinispan.filter.{KeyValueFilterConverterFactory, ParamKeyValueFilterConverterFactory}
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.notifications.Listener
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent
import org.infinispan.notifications.cachelistener.filter.{CacheEventConverterFactory, CacheEventFilterConverterFactory, CacheEventFilterFactory}
import org.infinispan.registry.InternalCacheRegistry
import org.infinispan.remoting.transport.Address
import org.infinispan.server.core.security.SaslUtils
import org.infinispan.server.core.transport.TimeoutEnabledChannelInitializer
import org.infinispan.server.core.{AbstractProtocolServer, QueryFacade}
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration
import org.infinispan.server.hotrod.event.KeyValueWithPreviousEventConverterFactory
import org.infinispan.server.hotrod.iteration.{DefaultIterationManager, IterationManager}
import org.infinispan.server.hotrod.logging.Log
import org.infinispan.server.hotrod.transport.HotRodChannelInitializer
import org.infinispan.upgrade.RollingUpgradeManager
import org.infinispan.util.concurrent.IsolationLevel
import org.infinispan.{AdvancedCache, IllegalLifecycleStateException}

import scala.collection.JavaConversions._

/**
 * Hot Rod server, in charge of defining its encoder/decoder and, if clustered, update the topology information
 * on startup and shutdown.
 *
 * TODO: It's too late for 5.1.1 series. In 5.2, split class into: local and cluster hot rod servers
 * This should safe some memory for the local case and the code should be cleaner
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class HotRodServer extends AbstractProtocolServer("HotRod") with Log {

   type SuitableConfiguration = HotRodServerConfiguration

   private var isClustered: Boolean = _
   private var clusterAddress: Address = _
   private var address: ServerAddress = _
   private var addressCache: AddressCache = _
   private val knownCaches = CollectionFactory.makeConcurrentMap[String, Cache](4, 0.9f, 16)
   private val knownCacheConfigurations = CollectionFactory.makeConcurrentMap[String, Configuration](4, 0.9f, 16)
   private val knownCacheRegistries = CollectionFactory.makeConcurrentMap[String, ComponentRegistry](4, 0.9f, 16)
   private var queryFacades: Seq[QueryFacade] = _
   private val saslMechFactories = CollectionFactory.makeConcurrentMap[String, SaslServerFactory](4, 0.9f, 16)
   private var clientListenerRegistry: ClientListenerRegistry = _
   private var marshaller: Marshaller = _
   private var distributedExecutorService: DefaultExecutorService = _
   private var viewChangeListener: CrashedMemberDetectorListener = _
   private var topologyChangeListener: ReAddMyAddressListener = _
   protected var executor: ExecutorService = _
   lazy val iterationManager: IterationManager = new DefaultIterationManager(getCacheManager)

   def getAddress: ServerAddress = address

   def getMarshaller = marshaller

   def query(cache: AdvancedCache[Array[Byte], Array[Byte]], query: Array[Byte]): Array[Byte] = {
      queryFacades.head.query(cache, query)
   }

   def getClientListenerRegistry: ClientListenerRegistry = clientListenerRegistry

   override def getEncoder = new HotRodEncoder(getCacheManager, this)

   override def getDecoder : HotRodDecoder =
      new HotRodDecoder(cacheManager, transport, this, new Predicate[String] {
               override def test(t: String): Boolean = isCacheIgnored(t)
            })

   override def startInternal(configuration: HotRodServerConfiguration, cacheManager: EmbeddedCacheManager) {
      // These are also initialized by super.startInternal, but we need them before
      this.configuration = configuration
      this.cacheManager = cacheManager

      // populate the sasl factories based on the required mechs
      setupSasl

      // Initialize query-specific stuff
      queryFacades = loadQueryFacades()
      clientListenerRegistry = new ClientListenerRegistry(configuration)

      addCacheEventConverterFactory("key-value-with-previous-converter-factory", new KeyValueWithPreviousEventConverterFactory)
      loadFilterConverterFactories(classOf[ParamKeyValueFilterConverterFactory[Any, Any, Any]])((name, f) => addKeyValueFilterConverterFactory(name, f.asInstanceOf[KeyValueFilterConverterFactory[_, _, _]]))
      loadFilterConverterFactories(classOf[CacheEventFilterConverterFactory])(addCacheEventFilterConverterFactory)
      loadFilterConverterFactories(classOf[CacheEventConverterFactory])(addCacheEventConverterFactory)
      loadFilterConverterFactories(classOf[KeyValueFilterConverterFactory[Any, Any, Any]])(addKeyValueFilterConverterFactory)

      // Start default cache and the endpoint before adding self to
      // topology in order to avoid topology updates being used before
      // endpoint is available.
      super.startInternal(configuration, cacheManager)

      // Add self to topology cache last, after everything is initialized
      val globalConfig = cacheManager.getCacheManagerConfiguration
      isClustered = globalConfig.transport().transport() != null
      if (isClustered) {
         defineTopologyCacheConfig(cacheManager)
         if (isDebugEnabled)
            debug("Externally facing address is %s:%d", configuration.proxyHost, configuration.proxyPort)

         addSelfToTopologyView(cacheManager)
      }
   }

   val abortPolicy = new AbortPolicy {
      override def rejectedExecution(r: Runnable, e: ThreadPoolExecutor) = {
         if (executor.isShutdown)
            throw new IllegalLifecycleStateException("Server has been stopped")
         else
            super.rejectedExecution(r, e)
      }
   }

   def getExecutor(threadPrefix: String) = {
      if (this.executor == null || this.executor.isShutdown) {
         val factory = new DefaultThreadFactory(threadPrefix + "ServerHandler")
         this.executor = new ThreadPoolExecutor(
            getConfiguration.workerThreads(),
            getConfiguration.workerThreads(),
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue[Runnable],
            factory,
            abortPolicy)
      }
      executor
   }

   override def getInitializer: ChannelInitializer[Channel] = {
      // Pass by name since we have circular dependency
      def getTransport() = {
         transport
      }
      if (configuration.idleTimeout > 0)
         new HotRodChannelInitializer(this, getTransport(), getEncoder, getExecutor(getQualifiedName()))
           with TimeoutEnabledChannelInitializer
      else // Idle timeout logic is disabled with -1 or 0 values
         new HotRodChannelInitializer(this, getTransport(), getEncoder, getExecutor(getQualifiedName()))
   }

   private def loadFilterConverterFactories[T](c: Class[T])(action: (String, T) => Any) = ServiceFinder.load(c).foreach { factory =>
      Option(factory.getClass.getAnnotation(classOf[org.infinispan.filter.NamedFactory])).foreach { ann =>
         action(ann.name, factory)
      }
   }

   private def loadQueryFacades(): Seq[QueryFacade] =
      ServiceLoader.load(classOf[QueryFacade], getClass().getClassLoader()).toSeq

   override def startTransport() {
      // Start predefined caches
      preStartCaches()

      super.startTransport()
   }

   override def startDefaultCache = {
      val cache = cacheManager.getCache[AnyRef, AnyRef](configuration.defaultCacheName())
      validateCacheConfiguration(cache.getCacheConfiguration)
      cache
   }

   private def preStartCaches() {
      // Start defined caches to avoid issues with lazily started caches. Skip internal caches if authorization is not
      // enabled
      val icr = cacheManager.getGlobalComponentRegistry.getComponent(classOf[InternalCacheRegistry])
      val authz = cacheManager.getCacheManagerConfiguration.security.authorization.enabled
      for (cacheName <- asScalaIterator(cacheManager.getCacheNames.iterator)) {
         val cache = getCacheInstance(cacheName, cacheManager, false, (!icr.internalCacheHasFlag(cacheName, InternalCacheRegistry.Flag.PROTECTED) || authz))
         val cacheCfg = SecurityActions.getCacheConfiguration(cache)
         validateCacheConfiguration(cacheCfg)
      }
   }

   private def validateCacheConfiguration(cacheCfg: Configuration) {
      val isolationLevel = cacheCfg.locking().isolationLevel()
      if ((isolationLevel == IsolationLevel.REPEATABLE_READ || isolationLevel == IsolationLevel.SERIALIZABLE) && !cacheCfg.locking().writeSkewCheck())
         throw log.invalidIsolationLevel(isolationLevel)
   }

   private def addSelfToTopologyView(cacheManager: EmbeddedCacheManager) {
      addressCache = cacheManager.getCache(configuration.topologyCacheName)
      clusterAddress = cacheManager.getAddress
      address = new ServerAddress(configuration.proxyHost, configuration.proxyPort)
      distributedExecutorService = new DefaultExecutorService(addressCache)

      viewChangeListener = new CrashedMemberDetectorListener(addressCache, this)
      cacheManager.addListener(viewChangeListener)
      topologyChangeListener = new ReAddMyAddressListener(addressCache, clusterAddress, address)
      addressCache.addListener(topologyChangeListener)

      // Map cluster address to server endpoint address
      debug("Map %s cluster address with %s server endpoint in address cache", clusterAddress, address)
      // Guaranteed delivery required since if data is lost, there won't be
      // any further cache calls, so negative acknowledgment can cause issues.
      addressCache.getAdvancedCache.withFlags(Flag.SKIP_CACHE_LOAD, Flag.GUARANTEED_DELIVERY)
              .put(clusterAddress, address)
   }

   private def defineTopologyCacheConfig(cacheManager: EmbeddedCacheManager) {
      val internalCacheRegistry = cacheManager.getGlobalComponentRegistry.getComponent(classOf[InternalCacheRegistry])
      internalCacheRegistry.registerInternalCache(configuration.topologyCacheName,
          createTopologyCacheConfig(cacheManager.getCacheManagerConfiguration.transport().distributedSyncTimeout()).build(),
          EnumSet.of(InternalCacheRegistry.Flag.EXCLUSIVE))
   }

   protected def createTopologyCacheConfig(distSyncTimeout: Long): ConfigurationBuilder = {
      val builder = new ConfigurationBuilder
      builder.clustering().cacheMode(CacheMode.REPL_SYNC).remoteTimeout(configuration.topologyReplTimeout)
             .locking().lockAcquisitionTimeout(configuration.topologyLockTimeout)
             .eviction().strategy(EvictionStrategy.NONE)
             .expiration().lifespan(-1).maxIdle(-1)
             // Topology cache uses Object based equals/hashCodes
             .dataContainer()
                .keyEquivalence(AnyEquivalence.getInstance())
                .valueEquivalence(AnyEquivalence.getInstance())

      if (configuration.topologyStateTransfer) {
         builder
            .clustering()
               .stateTransfer()
                  .awaitInitialTransfer(configuration.topologyAwaitInitialTransfer)
                  .fetchInMemoryState(true)
                  .timeout(distSyncTimeout + configuration.topologyReplTimeout)
      } else {
         builder.persistence().addClusterLoader().remoteCallTimeout(configuration.topologyReplTimeout)
      }

      builder
   }

   def getKnownCacheInstance(cacheName: String) = {
      knownCaches.get(cacheName)
   }

   def getCacheInstance(cacheName: String, cacheManager: EmbeddedCacheManager, skipCacheCheck: Boolean, addToKnownCaches: Boolean = true): Cache = {
      var cache: Cache = null
      if (!skipCacheCheck) cache = knownCaches.get(cacheName)

      if (cache == null) {
         val validCacheName = if (cacheName.isEmpty) configuration.defaultCacheName else cacheName
         val tmpCache = SecurityActions.getCache[Bytes, Bytes](cacheManager, validCacheName)
         val cacheConfiguration = SecurityActions.getCacheConfiguration(tmpCache.getAdvancedCache)
         val compatibility = cacheConfiguration.compatibility().enabled()
         val indexing = cacheConfiguration.indexing().index().isEnabled

         // Use flag when compatibility is enabled, otherwise it's unnecessary
         if (compatibility || indexing)
            cache = tmpCache.getAdvancedCache.withFlags(Flag.OPERATION_HOTROD)
         else
            cache = tmpCache.getAdvancedCache

         // We don't need synchronization as long as we store the cache last
         knownCacheConfigurations.put(cacheName, cacheConfiguration)
         knownCacheRegistries.put(cacheName, SecurityActions.getCacheComponentRegistry(tmpCache.getAdvancedCache))
         if (addToKnownCaches) {
            knownCaches.put(cacheName, cache)
         }
         // make sure we register a Migrator for this cache!
         tryRegisterMigrationManager(cacheName, cache)
      }

      cache
   }

   def getCacheConfiguration(cacheName: String): Configuration = {
      knownCacheConfigurations.get(cacheName)
   }

   def getCacheRegistry(cacheName: String): ComponentRegistry = {
      knownCacheRegistries.get(cacheName)
   }

   def tryRegisterMigrationManager(cacheName: String, cache: Cache) {
      val cr = SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache)
      val migrationManager = cr.getComponent(classOf[RollingUpgradeManager])
      if (migrationManager != null) migrationManager.addSourceMigrator(new HotRodSourceMigrator(cache))
   }

   private def setupSasl {
      val saslFactories = SaslUtils.getSaslServerFactories(this.getClass().getClassLoader(), true)
      while (saslFactories.hasNext) {
         val saslFactory = saslFactories.next
         val saslFactoryMechs = saslFactory.getMechanismNames(configuration.authentication.mechProperties)
         for (supportedMech <- saslFactoryMechs) {
            for (mech <- configuration.authentication.allowedMechs) {
               if (supportedMech == mech) {
                  saslMechFactories.putIfAbsent(mech, saslFactory)
               }
            }
         }
      }
   }

   def getSaslServerFactory(mech: String): SaslServerFactory = {
      saslMechFactories.get(mech)
   }

   private[hotrod] def getAddressCache = addressCache

   def addCacheEventFilterFactory(name: String, factory: CacheEventFilterFactory): Unit = {
      clientListenerRegistry.addCacheEventFilterFactory(name, factory)
   }

   def removeCacheEventFilterFactory(name: String): Unit = {
      clientListenerRegistry.removeCacheEventFilterFactory(name)
   }

   def addCacheEventConverterFactory(name: String, factory: CacheEventConverterFactory): Unit = {
      clientListenerRegistry.addCacheEventConverterFactory(name, factory)
   }

   def removeCacheEventConverterFactory(name: String): Unit = {
      clientListenerRegistry.removeCacheEventConverterFactory(name)
   }

   def addCacheEventFilterConverterFactory(name: String, factory: CacheEventFilterConverterFactory): Unit = {
      clientListenerRegistry.addCacheEventFilterConverterFactory(name, factory)
   }

   def removeCacheEventFilterConverterFactory(name: String): Unit = {
      clientListenerRegistry.removeCacheEventFilterConverterFactory(name)
   }

   def setMarshaller(marshaller: Marshaller): Unit = {
      this.marshaller = marshaller
      clientListenerRegistry.setEventMarshaller(Option(marshaller))
      iterationManager.setMarshaller(Option(marshaller))
   }

   def addKeyValueFilterConverterFactory[K, V, C](name: String, factory: KeyValueFilterConverterFactory[K, V, C]): Unit = {
      iterationManager.addKeyValueFilterConverterFactory(name, factory)
   }

   def removeKeyValueFilterConverterFactory[K, V, C](name: String): Unit = {
      iterationManager.removeKeyValueFilterConverterFactory(name)
   }

   override def stop: Unit = {
      if (viewChangeListener != null) {
         SecurityActions.removeListener(cacheManager, viewChangeListener)
      }
      if (topologyChangeListener != null) {
         SecurityActions.removeListener(addressCache, topologyChangeListener)
      }
      if (distributedExecutorService != null) {
         distributedExecutorService.shutdownNow()
      }

      if (clientListenerRegistry != null) clientListenerRegistry.stop()
      if (executor != null) executor.shutdownNow()
      super.stop
   }

   @Listener(sync = false)
   class ReAddMyAddressListener(addressCache: AddressCache, clusterAddress: Address, address: ServerAddress) {
      @TopologyChanged
      def topologyChanged(event: TopologyChangedEvent[Address, ServerAddress]): Unit = {
         if (event.isPre)
            return

         var success = false
         while (!success && !distributedExecutorService.isShutdown && addressCache.getStatus.allowInvocations()) {
            try {
               val futures = distributedExecutorService.submitEverywhere(new CheckAddressTask(clusterAddress, address))
               // No need for a timeout here, the distributed executor has a default task timeout
               val everybodyHasIt = futures.forall(_.get())
               if (!everybodyHasIt) {
                  log.debugf("Re-adding %s to the topology cache", clusterAddress)
                  addressCache.putAsync(clusterAddress, address)
               }
               success = true
            } catch {
               case e: Throwable => log.debug("Error re-adding address to topology cache, retrying", e)
            }
         }
      }
   }
}

object HotRodServer {
   val DEFAULT_TOPOLOGY_ID = -1
}

class CheckAddressTask(clusterAddress: Address, serverAddress: ServerAddress)
      extends DistributedCallable[Address, ServerAddress, Boolean] with Serializable with Log {
   @transient
   private var cache: infinispan.Cache[Address, ServerAddress] = null

   override def setEnvironment(cache: infinispan.Cache[Address, ServerAddress], inputKeys: java.util.Set[Address]): Unit = {
      this.cache = cache
   }

   override def call(): Boolean = {
      cache.containsKey(clusterAddress)
   }
}
