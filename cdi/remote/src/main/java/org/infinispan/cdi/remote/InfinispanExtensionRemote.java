package org.infinispan.cdi.remote;

import org.infinispan.cdi.common.util.AnyLiteral;
import org.infinispan.cdi.common.util.BeanBuilder;
import org.infinispan.cdi.common.util.ContextualLifecycle;
import org.infinispan.cdi.common.util.DefaultLiteral;
import org.infinispan.cdi.common.util.Reflections;
import org.infinispan.cdi.remote.logging.RemoteLog;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.logging.LogFactory;

import javax.enterprise.context.ApplicationScoped;
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
import javax.enterprise.inject.spi.ProcessAnnotatedType;
import javax.enterprise.inject.spi.ProcessInjectionTarget;
import javax.enterprise.inject.spi.ProcessProducer;
import javax.enterprise.inject.spi.Producer;
import javax.enterprise.util.AnnotationLiteral;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class InfinispanExtensionRemote implements Extension {

    private static final RemoteLog LOGGER = LogFactory.getLog(InfinispanExtensionRemote.class, RemoteLog.class);

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
        if (RemoteCacheManager.class.getCanonicalName().equals(bean.getAnnotatedType().getJavaClass().getCanonicalName())) {
            LOGGER.info("removing duplicated  RemoteCacheManager" + bean.getAnnotatedType());
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
                qualifiers.add(new AnnotationLiteral<Default>() {
                });
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
    <T, X> void registerBeans(@Observes AfterBeanDiscovery event, final BeanManager beanManager) {

        if (beanManager.getBeans(RemoteCacheManager.class).isEmpty()) {
            LOGGER.addDefaultRemoteCacheManager();
            event.addBean(createDefaultRemoteCacheManagerBean(beanManager));
        }

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

    /**
     * The default remote cache manager can be overridden by creating a producer which produces the new default remote
     * cache manager. The remote cache manager produced must have the scope {@link ApplicationScoped} and the
     * {@linkplain javax.enterprise.inject.Default Default} qualifier.
     *
     * @param beanManager
     * @return a custom bean
     */
    private Bean<RemoteCacheManager> createDefaultRemoteCacheManagerBean(BeanManager beanManager) {
        return new BeanBuilder<RemoteCacheManager>(beanManager).beanClass(InfinispanExtensionRemote.class)
                .addTypes(Object.class, RemoteCacheManager.class).scope(ApplicationScoped.class)
                .qualifiers(DefaultLiteral.INSTANCE, AnyLiteral.INSTANCE)
                .beanLifecycle(new ContextualLifecycle<RemoteCacheManager>() {

                    @Override
                    public RemoteCacheManager create(Bean<RemoteCacheManager> bean,
                                                     CreationalContext<RemoteCacheManager> creationalContext) {
                        return new RemoteCacheManager();
                    }

                    @Override
                    public void destroy(Bean<RemoteCacheManager> bean, RemoteCacheManager instance,
                                        CreationalContext<RemoteCacheManager> creationalContext) {
                        instance.stop();
                    }
                }).create();
    }

}
