package org.infinispan.marshall.protostream.impl;

import static org.infinispan.marshall.protostream.impl.SerializationContextRegistry.MarshallerType.GLOBAL;
import static org.infinispan.marshall.protostream.impl.SerializationContextRegistry.MarshallerType.PERSISTENCE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.UserContextInitializerImpl;
import org.infinispan.commons.util.ServiceFinder;
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
import org.infinispan.protostream.types.java.CommonContainerTypesSchema;
import org.infinispan.protostream.types.java.CommonTypesSchema;

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
      CommonTypesSchema commonTypesSchema = new CommonTypesSchema();
      register(commonTypesSchema, user);
      CommonContainerTypesSchema commonContainerTypesSchema = new CommonContainerTypesSchema();
      register(commonContainerTypesSchema, user);
      UserContextInitializerImpl userContextInitializer = new UserContextInitializerImpl();
      register(userContextInitializer, user);

      // Add user configured SCIs
      Collection<SerializationContextInitializer> initializers = globalConfig.serialization().contextInitializers();
      if (initializers == null || initializers.isEmpty()) {
         // If no SCIs have been explicitly configured, then load all available SCI services
         initializers = ServiceFinder.load(SerializationContextInitializer.class, globalConfig.classLoader());
      }
      initializers.forEach(sci -> register(sci, user));

      String messageName = PersistenceContextInitializer.getFqTypeName(MarshallableUserObject.class);
      BaseMarshaller userObjectMarshaller = new MarshallableUserObject.Marshaller(messageName, userMarshaller.wired());
      update(GLOBAL, ctx -> ctx.addContextInitializer(commonTypesSchema)
            .addContextInitializer(commonContainerTypesSchema)
            .addContextInitializer(new PersistenceContextInitializerImpl())
            // Register Commons util so that KeyValueWithPrevious can be used with JCache remote
            .addContextInitializer(new org.infinispan.commons.GlobalContextInitializerImpl())
            .addMarshaller(userObjectMarshaller)
            .update()
      );

      update(PERSISTENCE, ctx -> ctx.addContextInitializer(commonTypesSchema)
            .addContextInitializer(commonContainerTypesSchema)
            .addContextInitializer(new PersistenceContextInitializerImpl())
            .addMarshaller(userObjectMarshaller)
            .update()
      );
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
      update(type, ctx -> ctx.addContextInitializer(sci).update());
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

   private static void register(SerializationContextInitializer sci, SerializationContext ctx) {
      sci.registerSchema(ctx);
      sci.registerMarshallers(ctx);
   }

   // Required until IPROTO-136 is resolved to ensure that custom marshaller implementations are not overridden by
   // non-core modules registering their SerializationContextInitializer(s) which depend on a core initializer.
   private static final class MarshallerContext {
      private final List<SerializationContextInitializer> initializers = new ArrayList<>();
      private final List<FileDescriptorSource> schemas = new ArrayList<>();
      private final List<BaseMarshaller<?>> marshallers = new ArrayList<>();
      private final SerializationContext ctx = ProtobufUtil.newSerializationContext();

      MarshallerContext addContextInitializer(SerializationContextInitializer sci) {
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
