package org.infinispan.marshall.protostream.impl;

import static org.infinispan.marshall.protostream.impl.SerializationContextRegistry.MarshallerType.GLOBAL;
import static org.infinispan.marshall.protostream.impl.SerializationContextRegistry.MarshallerType.PERSISTENCE;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.persistence.impl.PersistenceContextInitializer;
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
   @Inject @ComponentName(KnownComponentNames.USER_MARSHALLER)
   ComponentRef<Marshaller> userMarshaller;

   private final MarshallerContext global = new MarshallerContext();
   private final MarshallerContext persistence = new MarshallerContext();
   private final SerializationContext user = ProtobufUtil.newSerializationContext();

   @Start
   public void start() {
      // Add user configured SCIs
      List<SerializationContextInitializer> scis = globalConfig.serialization().contextInitializers();
      if (scis != null) {
         scis.forEach(sci -> register(sci, user));
      }

      String messageName = PersistenceContextInitializer.getFqTypeName(MarshallableUserObject.class);
      BaseMarshaller userObjectMarshaller = new MarshallableUserObject.Marshaller(messageName, userMarshaller.wired());
      update(GLOBAL, ctx -> {
         if (scis != null)
            ctx.addContextIntializers(scis);

         ctx.addContextIntializer(new PersistenceContextInitializerImpl())
               // Register Commons util so that KeyValueWithPrevious can be used with JCache remote
               .addContextIntializer(new org.infinispan.commons.GlobalContextInitializerImpl())
               .addMarshaller(userObjectMarshaller)
               .update();
      });

      update(PERSISTENCE, ctx -> {
         if (scis != null)
            ctx.addContextIntializers(scis);

         ctx.addContextIntializer(new PersistenceContextInitializerImpl())
               .addContextIntializer(PersistenceContextManualInitializer.INSTANCE)
               .addMarshaller(userObjectMarshaller)
               .update();
      });
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
   public ImmutableSerializationContext getUserCtx() {
      return user;
   }

   @Override
   public void addContextInitializer(MarshallerType type, SerializationContextInitializer sci) {
      update(type, ctx -> ctx.addContextIntializer(sci).update());
   }

   @Override
   public void addProtoFile(MarshallerType type, FileDescriptorSource fileDescriptorSource) {
      update(type, ctx -> ctx.addProtoFile(fileDescriptorSource).update());
   }

   @Override
   public void addMarshaller(MarshallerType type, BaseMarshaller marshaller) {
      update(type, ctx -> ctx.addMarshaller(marshaller).update());
   }

   private void update(MarshallerType type, Consumer<MarshallerContext> consumer) {
      if (type == GLOBAL) {
         synchronized (global) {
            consumer.accept(global);
         }
      } else {
         synchronized (persistence) {
            consumer.accept(persistence);
         }
      }
   }

   private static void register(SerializationContextInitializer sci, SerializationContext... ctxs) {
      for (SerializationContext ctx : ctxs) {
         sci.registerSchema(ctx);
         sci.registerMarshallers(ctx);
      }
   }

   // Required until IPROTO-136 is resolved to ensure that custom marshaller implementations are not overridden by
   // non-core modules registering their SerializationContextInitializer(s) which depend on a core initializer.
   static class MarshallerContext {
      private final List<SerializationContextInitializer> initializers = new ArrayList<>();
      private final List<FileDescriptorSource> schemas = new ArrayList<>();
      private final List<BaseMarshaller<?>> marshallers = new ArrayList<>();
      private final SerializationContext ctx = ProtobufUtil.newSerializationContext();

      MarshallerContext addContextIntializers(List<SerializationContextInitializer> scis) {
         initializers.addAll(scis);
         return this;
      }

      MarshallerContext addContextIntializer(SerializationContextInitializer sci) {
         initializers.add(sci);
         return this;
      }

      MarshallerContext addProtoFile(FileDescriptorSource fileDescriptorSource) {
         schemas.add(fileDescriptorSource);
         return this;
      }

      MarshallerContext addMarshaller(BaseMarshaller marshaller) {
         marshallers.add(marshaller);
         return this;
      }

      void update() {
         initializers.forEach(sci -> {
            sci.registerSchema(ctx);
            sci.registerMarshallers(ctx);
         });
         schemas.forEach(ctx::registerProtoFiles);
         marshallers.forEach(ctx::registerMarshaller);
      }
   }
}
