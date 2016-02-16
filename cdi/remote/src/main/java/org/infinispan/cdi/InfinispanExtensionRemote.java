package org.infinispan.cdi;

import org.infinispan.cdi.logging.RemoteLog;
import org.infinispan.cdi.util.BeanBuilder;
import org.infinispan.cdi.util.ContextualLifecycle;
import org.infinispan.cdi.util.Reflections;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.logging.LogFactory;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.spi.*;
import javax.enterprise.util.AnnotationLiteral;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class InfinispanExtensionRemote implements Extension {

   private static final RemoteLog logger = LogFactory.getLog(InfinispanExtensionRemote.class, RemoteLog.class);

   private final Map<Type, Set<Annotation>> remoteCacheInjectionPoints;

   private Producer<RemoteCache<?, ?>> remoteCacheProducer;


   public InfinispanExtensionRemote() {
      new ConfigurationBuilder(); // Attempt to initialize a hotrod client class
      this.remoteCacheInjectionPoints = new HashMap<Type, Set<Annotation>>();
   }

   void processProducers(@Observes ProcessProducer<?, ?> event) {
      AnnotatedMember<?> member = event.getAnnotatedMember();
      if (RemoteCacheProducer.class.equals(member.getDeclaringType().getBaseType())) {
         remoteCacheProducer = (Producer<RemoteCache<?, ?>>) event.getProducer();
      }
   }

   // This is a work around for CDI Uber Jar deployment. When Weld scans the classpath it  pick up RemoteCacheManager
   // (this is an implementation, not an interface, so it gets instantiated). As a result we get duplicated classes
   // in CDI BeanManager.
   @SuppressWarnings("unused")
   <T extends RemoteCacheManager> void removeDuplicatedRemoteCacheManager(@Observes ProcessAnnotatedType<T> bean) {
      if(RemoteCacheManager.class.getCanonicalName().equals(bean.getAnnotatedType().getJavaClass().getCanonicalName())) {
         logger.info("removing duplicated  RemoteCacheManager" + bean.getAnnotatedType());
         bean.veto();
      }
   }

   <T> void saveRemoteInjectionPoints(@Observes ProcessInjectionTarget<T> event, BeanManager beanManager) {
      final InjectionTarget<T> injectionTarget = event.getInjectionTarget();

      for (InjectionPoint injectionPoint : injectionTarget.getInjectionPoints()) {
         final Annotated annotated = injectionPoint.getAnnotated();
         final Type type = annotated.getBaseType();
         final Class<?> rawType = Reflections.getRawType(annotated.getBaseType());
         final Set<Annotation> qualifiers = Reflections.getQualifiers(beanManager, annotated.getAnnotations());

         if (rawType.equals(RemoteCache.class) && qualifiers.isEmpty()) {
            qualifiers.add(new AnnotationLiteral<Default>() {});
            addRemoteCacheInjectionPoint(type, qualifiers);

         } else if (!annotated.isAnnotationPresent(Remote.class)
               && Reflections.getMetaAnnotation(annotated, Remote.class) != null
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
                             .readFromType(beanManager.createAnnotatedType(Reflections.getRawType(entry.getKey())))
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
