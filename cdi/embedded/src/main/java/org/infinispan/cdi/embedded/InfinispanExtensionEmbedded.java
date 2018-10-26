package org.infinispan.cdi.embedded;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Dependent;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.ProcessBean;
import javax.enterprise.inject.spi.ProcessProducer;
import javax.enterprise.inject.spi.Producer;
import javax.enterprise.util.TypeLiteral;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cdi.common.util.AnyLiteral;
import org.infinispan.cdi.common.util.Arrays2;
import org.infinispan.cdi.common.util.BeanBuilder;
import org.infinispan.cdi.common.util.Beans;
import org.infinispan.cdi.common.util.ContextualLifecycle;
import org.infinispan.cdi.common.util.ContextualReference;
import org.infinispan.cdi.common.util.DefaultLiteral;
import org.infinispan.cdi.common.util.Reflections;
import org.infinispan.cdi.embedded.event.cachemanager.CacheManagerEventBridge;
import org.infinispan.cdi.embedded.util.logging.EmbeddedLog;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * The Infinispan CDI extension for embedded caches
 *
 * @author Pete Muir
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 */
public class InfinispanExtensionEmbedded implements Extension {

   private static final String CACHE_NAME = "CDIExtensionDefaultCacheManager";

   private static final EmbeddedLog LOGGER = LogFactory.getLog(InfinispanExtensionEmbedded.class, EmbeddedLog.class);

   private final Set<ConfigurationHolder> configurations;

   private volatile boolean registered = false;

   private final Object registerLock = new Object();

   private Set<Set<Annotation>> installedEmbeddedCacheManagers = new HashSet<>();

   public InfinispanExtensionEmbedded() {
      new ConfigurationBuilder(); // Attempt to initialize a core class
      this.configurations = new HashSet<>();
   }

    @SuppressWarnings("unchecked")
    void observeConfigurationProducers(@Observes ProcessProducer<?, Configuration> event, BeanManager beanManager) {
        final ConfigureCache annotation = event.getAnnotatedMember().getAnnotation(ConfigureCache.class);
        String configurationName = "";
        if (annotation != null) {
            configurationName = annotation.value();
        }
        configurations.add(new ConfigurationHolder(event.getProducer(), configurationName,
                Reflections.getQualifiers(beanManager, event.getAnnotatedMember().getAnnotations())));
    }

    public void observeEmbeddedCacheManagerBean(@Observes ProcessBean<?> processBean) {
        if (processBean.getBean().getTypes().contains(EmbeddedCacheManager.class)) {
            installedEmbeddedCacheManagers.add(processBean.getBean().getQualifiers());
        }
    }

   @SuppressWarnings("unchecked")
   <T, X> void registerBeans(@Observes AfterBeanDiscovery event, final BeanManager beanManager) {

       if (beanManager.getBeans(Configuration.class).isEmpty()) {
           LOGGER.addDefaultEmbeddedConfiguration();
           final Configuration defaultConfiguration = new ConfigurationBuilder().build();
           // Must be added after AdvancedCache producer registration - see also AdvancedCacheProducer.getDefaultAdvancedCache()
           configurations.add(new ConfigurationHolder(defaultConfiguration, "", defaultQualifiers()));
           event.addBean(createDefaultEmbeddedConfigurationBean(beanManager, defaultConfiguration));
       }

       for (final ConfigurationHolder holder : configurations) {
          // register a AdvancedCache producer for each configuration
          Bean<?> advancedCacheBean = new BeanBuilder(beanManager)
          .readFromType(beanManager.createAnnotatedType(AdvancedCache.class))
          .qualifiers(Beans.buildQualifiers(holder.getQualifiers()))
          .addType(new TypeLiteral<AdvancedCache<T, X>>() {}.getType())
          .addType(new TypeLiteral<Cache<T, X>>() {}.getType())
          .passivationCapable(true)
          .id(InfinispanExtensionEmbedded.class.getSimpleName() + "#" + AdvancedCache.class.getSimpleName() + "#" + Cache.class.getSimpleName())
          .beanLifecycle(new ContextualLifecycle<AdvancedCache<?, ?>>() {
              @Override
              public AdvancedCache<?, ?> create(Bean<AdvancedCache<?, ?>> bean,
                 CreationalContext<AdvancedCache<?, ?>> creationalContext) {
                 return new ContextualReference<AdvancedCacheProducer>(beanManager, AdvancedCacheProducer.class).create(Reflections.cast(creationalContext)).get().getAdvancedCache(holder.getName(), holder.getQualifiers());
              }
           }).create();
          event.addBean(advancedCacheBean);
      }

      if (beanManager.getBeans(EmbeddedCacheManager.class).isEmpty()) {
         LOGGER.addDefaultEmbeddedCacheManager();
         event.addBean(createDefaultEmbeddedCacheManagerBean(beanManager));
      }

   }

   <K, V> void registerInputCacheCustomBean(@Observes AfterBeanDiscovery event, BeanManager beanManager) {

      @SuppressWarnings("serial")
      TypeLiteral<Cache<K, V>> typeLiteral = new TypeLiteral<Cache<K, V>>() {};
      event.addBean(new BeanBuilder<Cache<K, V>>(beanManager)
               .readFromType(beanManager.createAnnotatedType(typeLiteral.getRawType()))
               .addType(typeLiteral.getType()).qualifiers(new InputLiteral())
               .beanLifecycle(new ContextualLifecycle<Cache<K, V>>() {

                  @Override
                  public Cache<K, V> create(Bean<Cache<K, V>> bean,
                           CreationalContext<Cache<K, V>> creationalContext) {
                     return ContextInputCache.get();
                  }
               }).create());
      @SuppressWarnings("serial")
      TypeLiteral<Collection<K>> typeLiteralKeys = new TypeLiteral<Collection<K>>() {};
      event.addBean(new BeanBuilder<Collection<K>>(beanManager)
            .readFromType(beanManager.createAnnotatedType(typeLiteralKeys.getRawType()))
            .addType(typeLiteralKeys.getType()).qualifiers(new InputLiteral())
            .beanLifecycle(new ContextualLifecycle<Collection<K>>() {

               @Override
               public Collection<K> create(Bean<Collection<K>> bean,
                                         CreationalContext<Collection<K>> creationalContext) {
                  return ContextInputCache.getKeys();
               }
            }).create());
   }

   public Set<InstalledCacheManager> getInstalledEmbeddedCacheManagers(BeanManager beanManager) {
       Set<InstalledCacheManager> installedCacheManagers = new HashSet<>();
       for (Set<Annotation> qualifiers : installedEmbeddedCacheManagers) {
           Bean<?> b = beanManager.resolve(beanManager.getBeans(EmbeddedCacheManager.class, qualifiers.toArray(Reflections.EMPTY_ANNOTATION_ARRAY)));
           EmbeddedCacheManager cm = (EmbeddedCacheManager) beanManager.getReference(b, EmbeddedCacheManager.class, beanManager.createCreationalContext(b));
           installedCacheManagers.add(new InstalledCacheManager(cm, qualifiers.contains(DefaultLiteral.INSTANCE)));
       }
       return installedCacheManagers;
   }

   public void registerCacheConfigurations(CacheManagerEventBridge eventBridge, Instance<EmbeddedCacheManager> cacheManagers, BeanManager beanManager) {
      if (!registered) {
         synchronized (registerLock) {
            if (!registered) {
               final CreationalContext<Configuration> ctx = beanManager.createCreationalContext(null);
               final EmbeddedCacheManager defaultCacheManager = cacheManagers.select(DefaultLiteral.INSTANCE).get();

               for (ConfigurationHolder holder : configurations) {
                  final String cacheName = holder.getName();
                  final Configuration cacheConfiguration = holder.getConfiguration(ctx);
                  final Set<Annotation> cacheQualifiers = holder.getQualifiers();

                  // if a specific cache manager is defined for this cache we use it
                  final Instance<EmbeddedCacheManager> specificCacheManager = cacheManagers.select(cacheQualifiers.toArray(new Annotation[cacheQualifiers.size()]));
                  final EmbeddedCacheManager cacheManager = specificCacheManager.isUnsatisfied() ? defaultCacheManager : specificCacheManager.get();

                  // the default configuration is registered by the default cache manager producer
                  if (!cacheName.trim().isEmpty()) {
                     if (cacheConfiguration != null) {
                        cacheManager.defineConfiguration(cacheName, cacheConfiguration);
                        LOGGER.cacheConfigurationDefined(cacheName, cacheManager);
                     } else if (!cacheManager.getCacheNames().contains(cacheName)) {
                        cacheManager.defineConfiguration(cacheName, cacheManager.getDefaultCacheConfiguration());
                        LOGGER.cacheConfigurationDefined(cacheName, cacheManager);
                     }
                  }

                  // register cache manager observers
                  eventBridge.registerObservers(cacheQualifiers, cacheName, cacheManager);
               }

               // only set registered to true at the end to keep other threads waiting until we have finished registration
               registered = true;
            }
         }
      }

   }

   /**
    * The default embedded cache configuration can be overridden by creating a producer which
    * produces the new default configuration. The configuration produced must have the scope
    * {@linkplain javax.enterprise.context.Dependent Dependent} and the
    * {@linkplain javax.enterprise.inject.Default Default} qualifier.
    *
    * @param beanManager
    * @return a custom bean
    */
   private Bean<Configuration> createDefaultEmbeddedConfigurationBean(BeanManager beanManager, final Configuration configuration) {
      return new BeanBuilder<Configuration>(beanManager).beanClass(InfinispanExtensionEmbedded.class)
            .addTypes(Object.class, Configuration.class)
            .scope(Dependent.class)
            .qualifiers(defaultQualifiers())
            .passivationCapable(true)
            .id(InfinispanExtensionEmbedded.class.getSimpleName() + "#" + Configuration.class.getSimpleName())
            .beanLifecycle(new ContextualLifecycle<Configuration>() {
               @Override
               public Configuration create(Bean<Configuration> bean,
                     CreationalContext<Configuration> creationalContext) {
                  return configuration;
               }
            }).create();
   }

   /**
    * The default cache manager is an instance of {@link DefaultCacheManager} initialized with the
    * default configuration (either produced by
    * {@link #createDefaultEmbeddedConfigurationBean(BeanManager)} or provided by user). The default
    * cache manager can be overridden by creating a producer which produces the new default cache
    * manager. The cache manager produced must have the scope {@link ApplicationScoped} and the
    * {@linkplain javax.enterprise.inject.Default Default} qualifier.
    *
    * @param beanManager
    * @return a custom bean
    */
   private Bean<EmbeddedCacheManager> createDefaultEmbeddedCacheManagerBean(BeanManager beanManager) {
      return new BeanBuilder<EmbeddedCacheManager>(beanManager).beanClass(InfinispanExtensionEmbedded.class)
            .addTypes(Object.class, EmbeddedCacheManager.class)
            .scope(ApplicationScoped.class)
            .qualifiers(defaultQualifiers())
            .passivationCapable(true)
            .id(InfinispanExtensionEmbedded.class.getSimpleName() + "#" + EmbeddedCacheManager.class.getSimpleName())
            .beanLifecycle(new ContextualLifecycle<EmbeddedCacheManager>() {

               @Override
               public EmbeddedCacheManager create(Bean<EmbeddedCacheManager> bean,
                     CreationalContext<EmbeddedCacheManager> creationalContext) {
                  GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder().globalJmxStatistics()
                        .cacheManagerName(CACHE_NAME).build();
                  @SuppressWarnings("unchecked")
                  Bean<Configuration> configurationBean = (Bean<Configuration>) beanManager
                        .resolve(beanManager.getBeans(Configuration.class));
                  Configuration defaultConfiguration = (Configuration) beanManager.getReference(configurationBean,
                        Configuration.class, beanManager.createCreationalContext(configurationBean));
                  return new DefaultCacheManager(globalConfiguration, defaultConfiguration);
               }

               @Override
               public void destroy(Bean<EmbeddedCacheManager> bean, EmbeddedCacheManager instance,
                     CreationalContext<EmbeddedCacheManager> creationalContext) {
                  instance.stop();
               }
            }).create();
   }

   private Set<Annotation> defaultQualifiers() {
      return Arrays2.asSet(DefaultLiteral.INSTANCE, AnyLiteral.INSTANCE);
   }

   static class ConfigurationHolder {

      private final Producer<Configuration> producer;
      private final Set<Annotation> qualifiers;
      private final String name;
      private final Configuration configuration;

      ConfigurationHolder(Producer<Configuration> producer, String name, Set<Annotation> qualifiers) {
         this(producer, qualifiers, name, null);
      }

      ConfigurationHolder(Configuration configuration, String name, Set<Annotation> qualifiers) {
         this(null, qualifiers, name, configuration);
      }

      private ConfigurationHolder(Producer<Configuration> producer, Set<Annotation> qualifiers, String name,
            Configuration configuration) {
         this.producer = producer;
         this.qualifiers = qualifiers;
         this.name = name;
         this.configuration = configuration;
      }

      public Producer<Configuration> getProducer() {
         return producer;
      }

      public String getName() {
         return name;
      }

      public Set<Annotation> getQualifiers() {
         return qualifiers;
      }

      Configuration getConfiguration(CreationalContext<Configuration> ctx) {
         return configuration != null ? configuration : producer.produce(ctx);
      }

   }

   public static class InstalledCacheManager {
      final EmbeddedCacheManager cacheManager;
      final boolean isDefault;

      InstalledCacheManager(EmbeddedCacheManager cacheManager, boolean aDefault) {
         this.cacheManager = cacheManager;
         isDefault = aDefault;
      }

      public EmbeddedCacheManager getCacheManager() {
         return cacheManager;
      }

      public boolean isDefault() {
         return isDefault;
      }
   }
}
