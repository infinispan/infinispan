package org.infinispan.marshall.protostream.impl;

import static org.infinispan.marshall.protostream.impl.SerializationContextRegistry.MarshallerType.GLOBAL;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.persistence.impl.PersistenceContextInitializerImpl;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;

@Scope(Scopes.GLOBAL)
public class SerializationContextRegistryImpl implements SerializationContextRegistry {

   @Inject GlobalConfiguration globalConfig;

   //TODO [anistor] we should update SerializationContext directly and not use any synchronization
   private final MarshallerContext global = new MarshallerContext();
   private final MarshallerContext persistence = new MarshallerContext();

   @Start
   public void start() {
      // Add user configured SCIs to both the global and persistence context
      List<SerializationContextInitializer> scis = globalConfig.serialization().contextInitializers();
      if (scis != null)
         global.addContextInitializers(scis);

      global.addContextInitializer(new PersistenceContextInitializerImpl())
            // Register Commons util so that KeyValueWithPrevious can be used with JCache remote
            .addContextInitializer(new org.infinispan.commons.GlobalContextInitializerImpl())
            .update();

      if (scis != null)
         persistence.addContextInitializers(scis);

      persistence.addContextInitializer(new PersistenceContextInitializerImpl())
                 .update();
   }

   @Override
   public ImmutableSerializationContext getGlobalCtx() {
      return global.ctx;
   }

   @Override
   public ImmutableSerializationContext getPersistenceCtx() {
      return persistence.ctx;
   }

   @Override
   public void addContextInitializer(MarshallerType type, SerializationContextInitializer sci) {
      MarshallerContext ctx = type == GLOBAL ? global : persistence;
      ctx.addContextInitializer(sci).update();
   }

   @Override
   public void addProtoFile(MarshallerType type, FileDescriptorSource fileDescriptorSource) {
      MarshallerContext ctx = type == GLOBAL ? global : persistence;
      ctx.addProtoFile(fileDescriptorSource).update();
   }

   @Override
   public void addMarshaller(MarshallerType type, BaseMarshaller<?> marshaller) {
      MarshallerContext ctx = type == GLOBAL ? global : persistence;
      ctx.addMarshaller(marshaller).update();
   }

   private static final class MarshallerContext {
      private final List<SerializationContextInitializer> initializers = new ArrayList<>();
      private final List<FileDescriptorSource> schemas = new ArrayList<>();
      private final List<BaseMarshaller<?>> marshallers = new ArrayList<>();
      private final SerializationContext ctx = ProtobufUtil.newSerializationContext();

      synchronized MarshallerContext addContextInitializers(List<SerializationContextInitializer> scis) {
         initializers.addAll(scis);
         return this;
      }

      synchronized MarshallerContext addContextInitializer(SerializationContextInitializer sci) {
         initializers.add(sci);
         return this;
      }

      synchronized MarshallerContext addProtoFile(FileDescriptorSource fileDescriptorSource) {
         schemas.add(fileDescriptorSource);
         return this;
      }

      synchronized MarshallerContext addMarshaller(BaseMarshaller<?> marshaller) {
         marshallers.add(marshaller);
         return this;
      }

      synchronized void update() {
         //TODO [anistor] the fact the we need to re-register everything each time is a symptom of a hidden problem. If we don't we see StoreTypeMultimapCacheTest failing
         initializers.forEach(sci -> {
            sci.registerSchema(ctx);
            sci.registerMarshallers(ctx);
         });
         schemas.forEach(ctx::registerProtoFiles);
         marshallers.forEach(ctx::registerMarshaller);
      }
   }
}
