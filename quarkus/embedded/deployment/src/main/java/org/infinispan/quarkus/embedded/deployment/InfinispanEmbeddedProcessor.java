package org.infinispan.quarkus.embedded.deployment;

import java.io.IOException;
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
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.configuration.cache.AbstractModuleConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.SerializedWith;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;
import org.infinispan.factories.impl.ModuleMetadataBuilder;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.marshall.exts.CollectionExternalizer;
import org.infinispan.marshall.exts.EnumExternalizer;
import org.infinispan.marshall.exts.EnumSetExternalizer;
import org.infinispan.marshall.exts.MapExternalizer;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.quarkus.embedded.runtime.InfinispanEmbeddedProducer;
import org.infinispan.quarkus.embedded.runtime.InfinispanEmbeddedRuntimeConfig;
import org.infinispan.quarkus.embedded.runtime.InfinispanRecorder;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexView;
import org.jgroups.conf.PropertyConverter;
import org.jgroups.protocols.Bundler;
import org.jgroups.protocols.LocalTransport;
import org.jgroups.protocols.MsgStats;
import org.jgroups.protocols.raft.AppendEntriesRequest;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.MessageProcessingPolicy;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ThreadPool;
import org.jgroups.util.Util;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.ApplicationIndexBuildItem;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.IndexDependencyBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageResourceBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageResourceBundleBuildItem;
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
    void setup(BuildProducer<FeatureBuildItem> feature, BuildProducer<ReflectiveClassBuildItem> reflectiveClass,
            BuildProducer<ServiceProviderBuildItem> serviceProvider, BuildProducer<AdditionalBeanBuildItem> additionalBeans,
            BuildProducer<NativeImageResourceBuildItem> resources, CombinedIndexBuildItem combinedIndexBuildItem,
            List<InfinispanReflectionExcludedBuildItem> excludedReflectionClasses,
            ApplicationIndexBuildItem applicationIndexBuildItem, BuildProducer<NativeImageResourceBundleBuildItem> bundles) {
        feature.produce(new FeatureBuildItem("infinispan-embedded"));

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

        // These are either default or required for marshalling
        resources.produce(new NativeImageResourceBuildItem(
                "org/infinispan/protostream/message-wrapping.proto",
                "protostream/common-java-types.proto",
                "protostream/common-java-container-types.proto",
                "proto/generated/user.commons.proto",
                "proto/generated/persistence.commons.proto",
                "proto/generated/persistence.core.proto",
                "proto/generated/global.commons.proto",
                "default-configs/default-jgroups-udp.xml",
                "default-configs/default-jgroups-tcp.xml",
                "default-configs/default-jgroups-kubernetes.xml",
                "default-configs/default-jgroups-ec2.xml",
                "default-configs/default-jgroups-google.xml",
                "default-configs/default-jgroups-azure.xml",
                "stacks/udp.xml",
                "stacks/tcp.xml",
                "stacks/tcp_mping/tcp1.xml",
                "stacks/tcp_mping/tcp2.xml"
        ));

        reflectiveClass.produce(new ReflectiveClassBuildItem(false, false, HashFunctionPartitioner.class));
        reflectiveClass.produce(new ReflectiveClassBuildItem(false, false, JGroupsTransport.class));

        // XML reflection classes
        reflectiveClass.produce(new ReflectiveClassBuildItem(false, false,
                "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl",
                "com.sun.org.apache.xerces.internal.jaxp.datatype.DatatypeFactoryImpl",
                "com.sun.org.apache.xalan.internal.xsltc.trax.TransformerFactoryImpl",
                "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl",
                "com.sun.xml.bind.v2.ContextFactory",
                "com.sun.xml.internal.bind.v2.ContextFactory",
                "com.sun.xml.internal.stream.XMLInputFactoryImpl"));

        bundles.produce(new NativeImageResourceBundleBuildItem("com.sun.org.apache.xerces.internal.impl.msg.XMLMessages"));

        CollectionExternalizer.getSupportedPrivateClasses()
                .forEach(ceClass -> reflectiveClass.produce(new ReflectiveClassBuildItem(false, false, ceClass)));
        MapExternalizer.getSupportedPrivateClasses()
                .forEach(ceClass -> reflectiveClass.produce(new ReflectiveClassBuildItem(false, false, ceClass)));

        new EnumExternalizer().getTypeClasses()
              .forEach(ceClass -> reflectiveClass.produce(new ReflectiveClassBuildItem(false, false, ceClass)));

        new EnumSetExternalizer().getTypeClasses()
              .forEach(ceClass -> reflectiveClass.produce(new ReflectiveClassBuildItem(false, false, ceClass)));

        Set<DotName> excludedClasses = new HashSet<>();
        excludedReflectionClasses.forEach(excludedBuildItem -> excludedClasses.add(excludedBuildItem.getExcludedClass()));

        IndexView combinedIndex = combinedIndexBuildItem.getIndex();

        // Add all the JGroups Protocols
        addReflectionForName(Protocol.class.getName(), false, combinedIndex, reflectiveClass, false, true, excludedClasses);

        // Add a bunch of JGroups components
        addReflectionForClass(PropertyConverter.class, combinedIndex, reflectiveClass, excludedClasses);
        // Bundler fields can be invoked via configuration so add those methods
        addReflectionForName(Bundler.class.getName(), Bundler.class.isInterface(), combinedIndex, reflectiveClass, false, true, excludedClasses);
        addReflectionForClass(LocalTransport.class, combinedIndex, reflectiveClass, excludedClasses);
        addReflectionForClass(MessageProcessingPolicy.class, combinedIndex, reflectiveClass, excludedClasses);
        reflectiveClass.produce(new ReflectiveClassBuildItem(true, true, DiagnosticsHandler.class));
        //addReflectionForClass(DiagnosticsHandler.class, combinedIndex, reflectiveClass, excludedClasses);
        reflectiveClass.produce(new ReflectiveClassBuildItem(true, true, MsgStats.class));
        //addReflectionForClass(MsgStats.class, combinedIndex, reflectiveClass, excludedClasses);
        reflectiveClass.produce(new ReflectiveClassBuildItem(true, true, ThreadPool.class));
        //addReflectionForClass(ThreadPool.class, combinedIndex, reflectiveClass, excludedClasses);
        reflectiveClass.produce(new ReflectiveClassBuildItem(true, true, AppendEntriesRequest.class));

        // Add all consistent hash factories
        addReflectionForClass(ConsistentHashFactory.class, combinedIndex, reflectiveClass, excludedClasses);

        // We have to add reflection for our own loaders and stores as well due to how configuration works
        addReflectionForClass(CacheLoader.class, combinedIndex, reflectiveClass, excludedClasses);
        addReflectionForClass(CacheWriter.class, combinedIndex, reflectiveClass, excludedClasses);

        addReflectionForClass(NonBlockingStore.class, combinedIndex, reflectiveClass, excludedClasses);

        // We have to include all of our interceptors - technically a custom one is installed before or after ISPN ones
        // If we don't want to support custom interceptors this should be removable
        // We use reflection to set fields from the properties so those must be exposed as well
        addReflectionForName(AsyncInterceptor.class.getName(), true, combinedIndex, reflectiveClass, false, true, excludedClasses);

        // We use our configuration builders for all of our supported loaders - this also handles user custom configuration
        // builders
        addReflectionForClass(StoreConfigurationBuilder.class, combinedIndex, reflectiveClass, excludedClasses);

        // We use reflection to load up the attributes for a store configuration
        addReflectionForName(StoreConfiguration.class.getName(), true, combinedIndex, reflectiveClass, true, false,
                excludedClasses);

        // We use reflection to find various configuration serializers
        addReflectionForClass(ConfigurationSerializer.class, combinedIndex, reflectiveClass, excludedClasses);

        // Also use reflection to create configuration builders for modules
        addReflectionForClass(AbstractModuleConfigurationBuilder.class, combinedIndex, reflectiveClass, excludedClasses);

        reflectiveClass.produce(new ReflectiveClassBuildItem(false, false, Util.AddressScope.class));

        // Add Infinispan and user listeners to reflection list
        Collection<AnnotationInstance> listenerInstances = combinedIndex.getAnnotations(
                DotName.createSimple(Listener.class.getName()));
        for (AnnotationInstance instance : listenerInstances) {
            AnnotationTarget target = instance.target();
            if (target.kind() == AnnotationTarget.Kind.CLASS) {
                DotName targetName = target.asClass().name();
                if (!excludedClasses.contains(targetName)) {
                    reflectiveClass.produce(new ReflectiveClassBuildItem(true, false, targetName.toString()));
                }
            }
        }

        // Handle the various events required by a cluster
        reflectiveClass.produce(new ReflectiveClassBuildItem(false, false, CacheEntryCreated.class));
        reflectiveClass.produce(new ReflectiveClassBuildItem(false, false, CacheEntryExpired.class));
        reflectiveClass.produce(new ReflectiveClassBuildItem(false, false, CacheEntryModified.class));
        reflectiveClass.produce(new ReflectiveClassBuildItem(false, false, CacheEntryRemoved.class));

        // Infinispan has quite a few classes annotated with SerializeWith and user can use this - in the future
        // it would be nice to not have this required for Infinispan classes
        Collection<AnnotationInstance> serializeWith = combinedIndex
                .getAnnotations(DotName.createSimple(SerializeWith.class.getName()));
        registerSerializeWith(serializeWith, reflectiveClass, excludedClasses);

        // Configuration serializes with classes loaded via serialization
        serializeWith = combinedIndex.getAnnotations(DotName.createSimple(SerializedWith.class.getName()));
        registerSerializeWith(serializeWith, reflectiveClass, excludedClasses);

        // This contains parts from the index from the app itself
        Index appOnlyIndex = applicationIndexBuildItem.getIndex();

        // We only register the app advanced externalizers as all of the Infinispan ones are explicitly defined
        addReflectionForClass(AdvancedExternalizer.class, appOnlyIndex, reflectiveClass, Collections.emptySet());
        // Due to the index not containing AbstractExternalizer it doesn't know that it implements AdvancedExternalizer
        // thus we also have to include classes that extend AbstractExternalizer
        addReflectionForClass(AbstractExternalizer.class, appOnlyIndex, reflectiveClass, Collections.emptySet());
    }

    private void registerSerializeWith(Collection<AnnotationInstance> serializeWith,
          BuildProducer<ReflectiveClassBuildItem> reflectiveClass, Set<DotName> excludedClasses) {
        for (AnnotationInstance instance : serializeWith) {
            AnnotationValue withValue = instance.value();
            String withValueString = withValue.asString();
            DotName targetSerializer = DotName.createSimple(withValueString);
            if (!excludedClasses.contains(targetSerializer)) {
                reflectiveClass.produce(new ReflectiveClassBuildItem(false, false, withValueString));
            }
        }
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
            reflectiveClass.produce(new ReflectiveClassBuildItem(methods, fields,
                    classInfos.stream().map(ClassInfo::toString).toArray(String[]::new)));
        }
    }

    @Record(ExecutionTime.RUNTIME_INIT)
    @BuildStep
    void configureRuntimeProperties(InfinispanRecorder recorder, InfinispanEmbeddedRuntimeConfig runtimeConfig) {
        recorder.configureRuntimeProperties(runtimeConfig);
    }

    @BuildStep
    ReflectiveClassBuildItem cacheClasses() throws IOException {
        return new ReflectiveClassBuildItem(false, false,
              "com.github.benmanes.caffeine.cache.SSLMW",
              "com.github.benmanes.caffeine.cache.PSMW"
        );
    }
}
