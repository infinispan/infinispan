package org.infinispan.cdi;

import static org.infinispan.cdi.util.Reflections.getMetaAnnotation;
import static org.infinispan.cdi.util.Reflections.getQualifiers;
import static org.infinispan.cdi.util.Reflections.getRawType;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.Annotated;
import javax.enterprise.inject.spi.AnnotatedMember;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.enterprise.inject.spi.InjectionTarget;
import javax.enterprise.inject.spi.ProcessInjectionTarget;
import javax.enterprise.inject.spi.ProcessProducer;
import javax.enterprise.inject.spi.Producer;
import javax.enterprise.util.AnnotationLiteral;

import org.infinispan.cdi.util.BeanBuilder;
import org.infinispan.cdi.util.ContextualLifecycle;
import org.infinispan.cdi.util.logging.Log;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.logging.LogFactory;

public class InfinispanExtensionRemote implements Extension {
   private static final Log log = LogFactory.getLog(InfinispanExtensionRemote.class, Log.class);
   private final Map<Type, Set<Annotation>> remoteCacheInjectionPoints;

   private Producer<RemoteCache<?, ?>> remoteCacheProducer;


   public InfinispanExtensionRemote() {
      new ConfigurationBuilder(); // Attempt to initialize a hotrod client class
      this.remoteCacheInjectionPoints = new HashMap<Type, Set<Annotation>>();
   }

   void processProducers(ProcessProducer<?, ?> event) {
      AnnotatedMember<?> member = event.getAnnotatedMember();
      if (RemoteCacheProducer.class.equals(member.getDeclaringType().getBaseType())) {
         remoteCacheProducer = (Producer<RemoteCache<?, ?>>) event.getProducer();
      }
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
   }
}
