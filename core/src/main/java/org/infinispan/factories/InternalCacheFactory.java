package org.infinispan.factories;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.CacheImpl;
import org.infinispan.cache.impl.EncoderCache;
import org.infinispan.cache.impl.SimpleCacheImpl;
import org.infinispan.cache.impl.StatsCollectingCache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.dataconversion.BinaryEncoder;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.commons.dataconversion.CompatModeEncoder;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.GlobalMarshallerEncoder;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.configuration.cache.JMXStatisticsConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.encoding.DataConversion;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.eviction.impl.ActivationManagerStub;
import org.infinispan.eviction.impl.PassivationManagerStub;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.jmx.CacheJmxRegistration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventManager;
import org.infinispan.notifications.cachelistener.cluster.impl.ClusterEventManagerStub;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerStub;
import org.infinispan.stats.impl.StatsCollector;
import org.infinispan.transaction.xa.recovery.RecoveryAdminOperations;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.infinispan.xsite.XSiteAdminOperations;

/**
 * An internal factory for constructing Caches.  Used by the {@link DefaultCacheManager}, this is not intended as public
 * API.
 * <p/>
 * This is a special instance of a {@link AbstractComponentFactory} which contains bootstrap information for the {@link
 * ComponentRegistry}.
 * <p/>
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 4.0
 */
public class InternalCacheFactory<K, V> extends AbstractNamedCacheComponentFactory {
   /**
    * This implementation clones the configuration passed in before using it.
    *
    * @param configuration           to use
    * @param globalComponentRegistry global component registry to attach the cache to
    * @param cacheName               name of the cache
    * @return a cache
    * @throws CacheConfigurationException if there are problems with the cfg
    */
   public Cache<K, V> createCache(Configuration configuration,
                                  GlobalComponentRegistry globalComponentRegistry,
                                  String cacheName) throws CacheConfigurationException {
      try {
         if (configuration.simpleCache()) {
            return createSimpleCache(configuration, globalComponentRegistry, cacheName);
         } else {
            return createAndWire(configuration, globalComponentRegistry, cacheName);
         }
      } catch (CacheConfigurationException ce) {
         throw ce;
      } catch (RuntimeException re) {
         throw re;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private Class<? extends Encoder> getEncoderClass(GlobalConfiguration globalConfiguration, Configuration configuration) {

      boolean compatEnabled = configuration.compatibility().enabled();
      boolean embeddedMode = Configurations.isEmbeddedMode(globalConfiguration);

      if (compatEnabled && !embeddedMode) {
         return CompatModeEncoder.class;
      }

      StorageType storageType = configuration.memory().storageType();

      if (storageType == StorageType.BINARY) {
         return BinaryEncoder.class;
      }
      if (storageType == StorageType.OFF_HEAP) {
         return GlobalMarshallerEncoder.class;
      }

      return IdentityEncoder.class;
   }

   protected AdvancedCache<K, V> createAndWire(Configuration configuration, GlobalComponentRegistry globalComponentRegistry,
                                               String cacheName) throws Exception {
      Class<? extends Encoder> encoderClass = getEncoderClass(globalComponentRegistry.getGlobalConfiguration(), configuration);
      DataConversion keyDataConversion = new DataConversion(encoderClass, ByteArrayWrapper.class);
      DataConversion valueDataConversion = new DataConversion(encoderClass, ByteArrayWrapper.class);

      StreamingMarshaller marshaller = globalComponentRegistry.getOrCreateComponent(StreamingMarshaller.class);

      AdvancedCache<K, V> cache = new CacheImpl<>(cacheName);

      cache = new EncoderCache<>(cache, keyDataConversion, valueDataConversion);

      bootstrap(cacheName, cache, configuration, globalComponentRegistry, marshaller);
      if (marshaller != null) {
         componentRegistry.wireDependencies(marshaller);
      }
      return cache;
   }

   private AdvancedCache<K, V> createSimpleCache(Configuration configuration, GlobalComponentRegistry globalComponentRegistry,
                                                 String cacheName) {
      AdvancedCache<K, V> cache;
      Class<? extends Encoder> encoderClass = getEncoderClass(globalComponentRegistry.getGlobalConfiguration(), configuration);

      JMXStatisticsConfiguration jmxStatistics = configuration.jmxStatistics();
      boolean statisticsAvailable = jmxStatistics != null && jmxStatistics.available();
      DataConversion keyDataConversion = new DataConversion(encoderClass, ByteArrayWrapper.class);
      DataConversion valueDataConversion = new DataConversion(encoderClass, ByteArrayWrapper.class);
      if (statisticsAvailable) {
         cache = new StatsCollectingCache<>(cacheName, keyDataConversion, valueDataConversion);
      } else {
         cache = new SimpleCacheImpl<>(cacheName, keyDataConversion, valueDataConversion);
      }
      this.configuration = configuration;

      cache = new EncoderCache<>(cache, keyDataConversion, valueDataConversion);

      componentRegistry = new ComponentRegistry(cacheName, configuration, cache, globalComponentRegistry, globalComponentRegistry.getClassLoader()) {
         @Override
         protected void bootstrapComponents() {
            if (statisticsAvailable) {
               registerComponent(new StatsCollector.Factory(), StatsCollector.Factory.class);
            }
            registerComponent(new ClusterEventManagerStub<K, V>(), ClusterEventManager.class);
            registerComponent(new PassivationManagerStub(), PassivationManager.class);
            registerComponent(new ActivationManagerStub(), ActivationManager.class);
            registerComponent(new PersistenceManagerStub(), PersistenceManager.class);
         }

         @Override
         public void cacheComponents() {
            getOrCreateComponent(ExpirationManager.class);
         }
      };

      componentRegistry.registerComponent(new CacheJmxRegistration(), CacheJmxRegistration.class.getName(), true);
      componentRegistry.registerComponent(new RollingUpgradeManager(), RollingUpgradeManager.class.getName(), true);
      componentRegistry.registerComponent(cache, Cache.class.getName(), true);

      return cache;
   }


   /**
    * Bootstraps this factory with a Configuration and a ComponentRegistry.
    */
   private void bootstrap(String cacheName, AdvancedCache<?, ?> cache, Configuration configuration,
                          GlobalComponentRegistry globalComponentRegistry, StreamingMarshaller marshaller) {
      this.configuration = configuration;

      // injection bootstrap stuff
      componentRegistry = new ComponentRegistry(cacheName, configuration, cache, globalComponentRegistry, globalComponentRegistry.getClassLoader());

      /*
         --------------------------------------------------------------------------------------------------------------
         This is where the bootstrap really happens.  Registering the cache in the component registry will cause
         the component registry to look at the cache's @Inject methods, and construct various components and their
         dependencies, in turn.
         --------------------------------------------------------------------------------------------------------------
       */
      componentRegistry.registerComponent(cache, Cache.class.getName(), true);
      componentRegistry.registerComponent(new CacheJmxRegistration(), CacheJmxRegistration.class.getName(), true);
      if (configuration.transaction().recovery().enabled()) {
         componentRegistry.registerComponent(new RecoveryAdminOperations(), RecoveryAdminOperations.class.getName(), true);
      }
      if (configuration.sites().hasEnabledBackups()) {
         componentRegistry.registerComponent(new XSiteAdminOperations(), XSiteAdminOperations.class.getName(), true);
      }
      // The RollingUpgradeManager should always be added so it is registered in JMX.
      componentRegistry.registerComponent(new RollingUpgradeManager(), RollingUpgradeManager.class.getName(), true);
   }

   @Override
   public <T> T construct(Class<T> componentType) {
      throw new UnsupportedOperationException("Should never be invoked - this is a bootstrap factory.");
   }
}
