package org.infinispan.quarkus.embedded.deployment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.factories.impl.ModuleMetadataBuilder;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.notifications.Listener;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.quarkus.embedded.runtime.InfinispanEmbeddedProducer;
import org.infinispan.quarkus.embedded.runtime.InfinispanEmbeddedRuntimeConfig;
import org.infinispan.quarkus.embedded.runtime.InfinispanRecorder;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexView;

import com.github.benmanes.caffeine.cache.CacheLoader;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.ApplicationIndexBuildItem;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.IndexDependencyBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ServiceProviderBuildItem;

class InfinispanEmbeddedProcessor {

    @BuildStep
    void addInfinispanDependencies(BuildProducer<IndexDependencyBuildItem> indexDependency) {
        indexDependency.produce(new IndexDependencyBuildItem("org.jgroups", "jgroups"));
        indexDependency.produce(new IndexDependencyBuildItem("org.infinispan", "infinispan-commons"));
        indexDependency.produce(new IndexDependencyBuildItem("org.infinispan", "infinispan-core"));
    }

    @BuildStep
    void setup(BuildProducer<ReflectiveClassBuildItem> reflectiveClass,
            BuildProducer<ServiceProviderBuildItem> serviceProvider, BuildProducer<AdditionalBeanBuildItem> additionalBeans,
            CombinedIndexBuildItem combinedIndexBuildItem,
            List<InfinispanReflectionExcludedBuildItem> excludedReflectionClasses,
            ApplicationIndexBuildItem applicationIndexBuildItem) {

        additionalBeans.produce(AdditionalBeanBuildItem.unremovableOf(InfinispanEmbeddedProducer.class));

        for (Class<?> serviceLoadedInterface : Arrays.asList(ModuleMetadataBuilder.class, ConfigurationParser.class)) {
            // Need to register all the modules as service providers so they can be picked up at runtime
            ServiceLoader<?> serviceLoader = ServiceLoader.load(serviceLoadedInterface);
            List<String> interfaceImplementations = new ArrayList<>();
            serviceLoader.forEach(mmb -> interfaceImplementations.add(mmb.getClass().getName()));
            if (!interfaceImplementations.isEmpty()) {
                serviceProvider
                        .produce(new ServiceProviderBuildItem(serviceLoadedInterface.getName(), interfaceImplementations));
            }
        }

        Set<DotName> excludedClasses = new HashSet<>();
        excludedReflectionClasses.forEach(excludedBuildItem -> {
            excludedClasses.add(excludedBuildItem.getExcludedClass());
        });

        // This contains parts from the index from the app itself
        Index appOnlyIndex = applicationIndexBuildItem.getIndex();

        // Persistence SPIs
        IndexView combinedIndex = combinedIndexBuildItem.getIndex();

        // We need to use the CombinedIndex for these interfaces in order to discover implementations of the various
        // subclasses.
        addReflectionForClass(CacheLoader.class, combinedIndex, reflectiveClass, excludedClasses);
        addReflectionForClass(CacheWriter.class, combinedIndex, reflectiveClass, excludedClasses);
        addReflectionForClass(NonBlockingStore.class, combinedIndex, reflectiveClass, excludedClasses);
        addReflectionForName(AsyncInterceptor.class.getName(), true, combinedIndex, reflectiveClass, false, true, excludedClasses);

        // Add user listeners
        Collection<AnnotationInstance> listenerInstances = combinedIndex.getAnnotations(
                DotName.createSimple(Listener.class.getName())
        );

        for (AnnotationInstance instance : listenerInstances) {
            AnnotationTarget target = instance.target();
            if (target.kind() == AnnotationTarget.Kind.CLASS) {
                DotName targetName = target.asClass().name();
                if (!excludedClasses.contains(targetName)) {
                    reflectiveClass.produce(
                            ReflectiveClassBuildItem.builder(target.toString()).methods().build()
                    );
                }
            }
        }

        // We only register the app advanced externalizers as all of the Infinispan ones are explicitly defined
        addReflectionForClass(AdvancedExternalizer.class, appOnlyIndex, reflectiveClass, Collections.emptySet());
        // Due to the index not containing AbstractExternalizer it doesn't know that it implements AdvancedExternalizer
        // thus we also have to include classes that extend AbstractExternalizer
        addReflectionForClass(AbstractExternalizer.class, appOnlyIndex, reflectiveClass, Collections.emptySet());
    }

    private void addReflectionForClass(Class<?> classToUse, IndexView indexView,
                                       BuildProducer<ReflectiveClassBuildItem> reflectiveClass, Set<DotName> excludedClasses) {
        addReflectionForName(classToUse.getName(), classToUse.isInterface(), indexView, reflectiveClass, false, false,
                excludedClasses);
    }

    private void addReflectionForName(String className, boolean isInterface, IndexView indexView,
                                      BuildProducer<ReflectiveClassBuildItem> reflectiveClass, boolean methods, boolean fields,
                                      Set<DotName> excludedClasses) {
        Collection<ClassInfo> classInfos;
        if (isInterface) {
            classInfos = indexView.getAllKnownImplementors(DotName.createSimple(className));
        } else {
            classInfos = indexView.getAllKnownSubclasses(DotName.createSimple(className));
        }

        classInfos.removeIf(ci -> excludedClasses.contains(ci.name()));

        if (!classInfos.isEmpty()) {
            String[] classNames = classInfos.stream().map(ClassInfo::toString).toArray(String[]::new);
            reflectiveClass.produce(
                    ReflectiveClassBuildItem.builder(classNames)
                            .methods(methods)
                            .fields(fields)
                            .build()
            );
        }
    }

    @Record(ExecutionTime.RUNTIME_INIT)
    @BuildStep
    void configureRuntimeProperties(InfinispanRecorder recorder, InfinispanEmbeddedRuntimeConfig runtimeConfig) {
        recorder.configureRuntimeProperties(runtimeConfig);
    }
}
