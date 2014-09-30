package org.infinispan.cdi;

import static org.infinispan.cdi.util.Reflections.getMetaAnnotation;
import static org.infinispan.cdi.util.Reflections.getQualifiers;
import static org.infinispan.cdi.util.Reflections.getRawType;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.Annotated;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.enterprise.inject.spi.InjectionTarget;
import javax.enterprise.inject.spi.ProcessBean;
import javax.enterprise.inject.spi.ProcessInjectionTarget;
import javax.enterprise.inject.spi.ProcessProducer;
import javax.enterprise.inject.spi.Producer;
import javax.enterprise.util.AnnotationLiteral;
import javax.enterprise.util.TypeLiteral;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cdi.event.cachemanager.CacheManagerEventBridge;
import org.infinispan.cdi.util.BeanBuilder;
import org.infinispan.cdi.util.Beans;
import org.infinispan.cdi.util.ContextualLifecycle;
import org.infinispan.cdi.util.ContextualReference;
import org.infinispan.cdi.util.DefaultLiteral;
import org.infinispan.cdi.util.Reflections;
import org.infinispan.cdi.util.defaultbean.DefaultBean;
import org.infinispan.cdi.util.defaultbean.DefaultBeanHolder;
import org.infinispan.cdi.util.defaultbean.Installed;
import org.infinispan.cdi.util.logging.Log;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.LogFactory;

/**
 * The Infinispan CDI extension class.
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class InfinispanExtension implements Extension {

   private Producer<RemoteCache<?, ?>> remoteCacheProducer;
   private static final Log log = LogFactory.getLog(InfinispanExtension.class, Log.class);

   private final Set<ConfigurationHolder> configurations;
   private final Map<Type, Set<Annotation>> remoteCacheInjectionPoints;

   private volatile boolean registered = false;
   private final Object registerLock = new Object();

   private Set<Set<Annotation>> installedEmbeddedCacheManagers = new HashSet<Set<Annotation>>();

   public InfinispanExtension() {
      this.configurations = new HashSet<InfinispanExtension.ConfigurationHolder>();
      this.remoteCacheInjectionPoints = new HashMap<Type, Set<Annotation>>();
   }

   void saveRemoteCacheProducer(@Observes ProcessProducer<RemoteCacheProducer, RemoteCache<?, ?>> event) {
      remoteCacheProducer = event.getProducer();
   }

   <T> void saveRemoteInjectionPoints(@Observes ProcessInjectionTarget<T> event, BeanManager beanManager) {
      final InjectionTarget<T> injectionTarget = event.getInjectionTarget();

      for (InjectionPoint injectionPoint : injectionTarget.getInjectionPoints()) {
         final Annotated annotated = injectionPoint.getAnnotated();
         final Type type = annotated.getBaseType();
         final Class<?> rawType = getRawType(annotated.getBaseType());
         final Set<Annotation> qualifiers = getQualifiers(beanManager, annotated.getAnnotations());

         if (rawType.equals(RemoteCache.class) && qualifiers.isEmpty()) {
            qualifiers.add(new AnnotationLiteral<Default>() {});
            addRemoteCacheInjectionPoint(type, qualifiers);

         } else if (!annotated.isAnnotationPresent(Remote.class)
               && getMetaAnnotation(annotated, Remote.class) != null
               && rawType.isAssignableFrom(RemoteCache.class)) {

            addRemoteCacheInjectionPoint(type, qualifiers);
         }
      }
   }

   private void addRemoteCacheInjectionPoint(Type type, Set<Annotation> qualifiers) {
      final Set<Annotation> currentQualifiers = remoteCacheInjectionPoints.get(type);

      if (currentQualifiers == null) {
         remoteCacheInjectionPoints.put(type, qualifiers);
      } else {
         currentQualifiers.addAll(qualifiers);
      }
   }

   void saveCacheConfigurations(@Observes ProcessProducer<?, Configuration> event, BeanManager beanManager) {
      final ConfigureCache annotation = event.getAnnotatedMember().getAnnotation(ConfigureCache.class);

      if (annotation != null) {
         configurations.add(new ConfigurationHolder(
               event.getProducer(),
               annotation.value(),
               getQualifiers(beanManager, event.getAnnotatedMember().getAnnotations())
         ));
      }
   }

   @SuppressWarnings("unchecked")
   <T, X>void registerCacheBeans(@Observes AfterBeanDiscovery event, final BeanManager beanManager) {
      for (Map.Entry<Type, Set<Annotation>> entry : remoteCacheInjectionPoints.entrySet()) {

         event.addBean(new BeanBuilder(beanManager)
                             .readFromType(beanManager.createAnnotatedType(getRawType(entry.getKey())))
                             .addType(entry.getKey())
                             .addQualifiers(entry.getValue())
                             .beanLifecycle(new ContextualLifecycle<RemoteCache<?, ?>>() {
                                @Override
                                public RemoteCache<?, ?> create(Bean<RemoteCache<?, ?>> bean, CreationalContext<RemoteCache<?, ?>> ctx) {
                                   return remoteCacheProducer.produce(ctx);
                                }

                                @Override
                                public void destroy(Bean<RemoteCache<?, ?>> bean, RemoteCache<?, ?> instance, CreationalContext<RemoteCache<?, ?>> ctx) {
                                   remoteCacheProducer.dispose(instance);
                                }
                             }).create());
      }
      for (final ConfigurationHolder holder : configurations) {
          // register a AdvancedCache producer for each
          Bean<?> b = new BeanBuilder(beanManager)
          .readFromType(beanManager.createAnnotatedType(AdvancedCache.class))
          .qualifiers(Beans.buildQualifiers(holder.getQualifiers()))
          .addType(new TypeLiteral<AdvancedCache<T, X>>() {}.getType())
          .addType(new TypeLiteral<Cache<T, X>>() {}.getType())
          .beanLifecycle(new ContextualLifecycle<AdvancedCache<?, ?>>() {
              @Override
              public AdvancedCache<?, ?> create(Bean<AdvancedCache<?, ?>> bean,
                 CreationalContext<AdvancedCache<?, ?>> creationalContext) {
                 return new ContextualReference<AdvancedCacheProducer>(beanManager, AdvancedCacheProducer.class).create(Reflections.<CreationalContext<AdvancedCacheProducer>>cast(creationalContext)).get().getAdvancedCache(holder.getName(), holder.getQualifiers());
              }

              @Override
              public void destroy(Bean<AdvancedCache<?, ?>> bean, AdvancedCache<?, ?> instance,
                 CreationalContext<AdvancedCache<?, ?>> creationalContext) {
                 // No-op, Infinispan manages the lifecycle

              }
           }).create();
          event.addBean(b);
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

                  @Override
                  public void destroy(Bean<Cache<K, V>> bean, Cache<K, V> instance,
                           CreationalContext<Cache<K, V>> creationalContext) {

                  }
               }).create());
   }

   public void observeDefaultEmbeddedCacheManagerInstalled(@Observes @Installed DefaultBeanHolder bean) {
       if (bean.getBean().getTypes().contains(EmbeddedCacheManager.class)) {
           installedEmbeddedCacheManagers.add(bean.getBean().getQualifiers());
       }
   }

   public Set<InstalledCacheManager> getInstalledEmbeddedCacheManagers(BeanManager beanManager) {
       Set<InstalledCacheManager> installedCacheManagers = new HashSet<InstalledCacheManager>();
       for (Set<Annotation> qualifiers : installedEmbeddedCacheManagers) {
           Bean<?> b = beanManager.resolve(beanManager.getBeans(EmbeddedCacheManager.class, qualifiers.toArray(Reflections.EMPTY_ANNOTATION_ARRAY)));
           EmbeddedCacheManager cm = (EmbeddedCacheManager) beanManager.getReference(b, EmbeddedCacheManager.class, beanManager.createCreationalContext(b));
           installedCacheManagers.add(new InstalledCacheManager(cm, qualifiers.contains(DefaultLiteral.INSTANCE)));
       }
       return installedCacheManagers;
   }

   public void observeEmbeddedCacheManagerBean(@Observes ProcessBean<?> processBean) {
       if (processBean.getBean().getTypes().contains(EmbeddedCacheManager.class) && !processBean.getAnnotated().isAnnotationPresent(DefaultBean.class)) {
           // Install any non-default EmbeddedCacheManager producers. We handle default ones separately, to ensure we only pick them up if installed
           installedEmbeddedCacheManagers.add(processBean.getBean().getQualifiers());
       }
   }

   public void registerCacheConfigurations(CacheManagerEventBridge eventBridge, Instance<EmbeddedCacheManager> cacheManagers, BeanManager beanManager) {
      if (!registered) {
         synchronized (registerLock) {
            if (!registered) {
               final CreationalContext<Configuration> ctx = beanManager.createCreationalContext(null);
               final EmbeddedCacheManager defaultCacheManager = cacheManagers.select(DefaultLiteral.INSTANCE).get();

               for (ConfigurationHolder oneConfigurationHolder : configurations) {
                  final String cacheName = oneConfigurationHolder.getName();
                  final Configuration cacheConfiguration = oneConfigurationHolder.getProducer().produce(ctx);
                  final Set<Annotation> cacheQualifiers = oneConfigurationHolder.getQualifiers();

                  // if a specific cache manager is defined for this cache we use it
                  final Instance<EmbeddedCacheManager> specificCacheManager = cacheManagers.select(cacheQualifiers.toArray(new Annotation[cacheQualifiers.size()]));
                  final EmbeddedCacheManager cacheManager = specificCacheManager.isUnsatisfied() ? defaultCacheManager : specificCacheManager.get();

                  // the default configuration is registered by the default cache manager producer
                  if (!cacheName.trim().isEmpty()) {
                     if (cacheConfiguration != null) {
                        cacheManager.defineConfiguration(cacheName, cacheConfiguration);
                        log.cacheConfigurationDefined(cacheName, cacheManager);
                     } else if (!cacheManager.getCacheNames().contains(cacheName)) {
                        cacheManager.defineConfiguration(cacheName, cacheManager.getDefaultCacheConfiguration());
                        log.cacheConfigurationDefined(cacheName, cacheManager);
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

   static class ConfigurationHolder {
      private final Producer<Configuration> producer;
      private final Set<Annotation> qualifiers;
      private final String name;

      ConfigurationHolder(Producer<Configuration> producer, String name, Set<Annotation> qualifiers) {
         this.producer = producer;
         this.name = name;
         this.qualifiers = qualifiers;
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
