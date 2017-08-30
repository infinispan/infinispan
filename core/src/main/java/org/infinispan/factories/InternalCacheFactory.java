package org.infinispan.factories;

import java.util.function.BiFunction;

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
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.configuration.cache.ContentTypeConfiguration;
import org.infinispan.configuration.cache.JMXStatisticsConfiguration;
import org.infinispan.configuration.cache.StorageType;
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
      } catch (RuntimeException re) {
         throw re;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private AdvancedCache<K, V> createAndWire(Configuration configuration, GlobalComponentRegistry globalComponentRegistry,
                                             String cacheName) throws Exception {
      StreamingMarshaller marshaller = globalComponentRegistry.getOrCreateComponent(StreamingMarshaller.class);

      boolean embeddedMode = Configurations.isEmbeddedMode(globalComponentRegistry.getGlobalConfiguration());

      AdvancedCache<K, V> cache = buildEncodingCache((kc, vc) -> new CacheImpl<>(cacheName), configuration, embeddedMode);

      bootstrap(cacheName, cache, configuration, globalComponentRegistry);
      if (marshaller != null) {
         componentRegistry.wireDependencies(marshaller);
      }
      return cache;
   }

   private AdvancedCache<K, V> buildEncodingCache(BiFunction<DataConversion, DataConversion,
         AdvancedCache<K, V>> wrappedCacheBuilder, Configuration configuration, boolean embeddedMode) {
      ContentTypeConfiguration keyEncodingConfig = configuration.encoding().keyDataType();
      ContentTypeConfiguration valueEncodingConfig = configuration.encoding().valueDataType();

      MediaType keyType = keyEncodingConfig.mediaType();
      MediaType valueType = valueEncodingConfig.mediaType();

      StorageType storageType = configuration.memory().storageType();

      Class<? extends Encoder> keyEncoderClass = IdentityEncoder.class;
      Class<? extends Encoder> valueEncoderClass = IdentityEncoder.class;

      boolean compatEnabled = configuration.compatibility().enabled();

      if (compatEnabled && !embeddedMode) {
         keyEncoderClass = CompatModeEncoder.class;
         valueEncoderClass = CompatModeEncoder.class;
      } else {
         if (storageType == StorageType.BINARY) {
            keyEncoderClass = BinaryEncoder.class;
            valueEncoderClass = BinaryEncoder.class;
         }
         if (embeddedMode && storageType == StorageType.OFF_HEAP) {
            keyEncoderClass = GlobalMarshallerEncoder.class;
            valueEncoderClass = GlobalMarshallerEncoder.class;
         }
      }

      DataConversion keyDataConversion = DataConversion.newKeyDataConversion(keyEncoderClass, ByteArrayWrapper.class, keyType);
      DataConversion valueDataConversion = DataConversion.newValueDataConversion(valueEncoderClass, ByteArrayWrapper.class, valueType);

      return new EncoderCache<>(wrappedCacheBuilder.apply(keyDataConversion, valueDataConversion), keyDataConversion, valueDataConversion);
   }

   private AdvancedCache<K, V> createSimpleCache(Configuration configuration, GlobalComponentRegistry globalComponentRegistry,
                                                 String cacheName) {
      AdvancedCache<K, V> cache;

      JMXStatisticsConfiguration jmxStatistics = configuration.jmxStatistics();
      boolean statisticsAvailable = jmxStatistics != null && jmxStatistics.available();
      if (statisticsAvailable) {
         cache = buildEncodingCache((kc, vc) -> new StatsCollectingCache<>(cacheName, kc, vc), configuration, true);
      } else {
         cache = buildEncodingCache((kc, vc) -> new SimpleCacheImpl<>(cacheName, kc, vc), configuration, true);
      }
      this.configuration = configuration;

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
                          GlobalComponentRegistry globalComponentRegistry) {
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
