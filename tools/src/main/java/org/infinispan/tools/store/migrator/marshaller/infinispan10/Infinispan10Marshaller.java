package org.infinispan.tools.store.migrator.marshaller.infinispan10;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.marshall.protostream.impl.MarshallableUserObject;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.tools.store.migrator.marshaller.common.AbstractUnsupportedStreamingMarshaller;

public class Infinispan10Marshaller extends AbstractUnsupportedStreamingMarshaller {

   final static Set<SerializationContextInitializer> internalSCIs = new HashSet<>();
   static {
      internalSCIs.add(new org.infinispan.commons.marshall.PersistenceContextInitializerImpl());
      internalSCIs.add(new org.infinispan.marshall.persistence.impl.PersistenceContextInitializerImpl());
      internalSCIs.add(new org.infinispan.multimap.impl.PersistenceContextInitializerImpl());
      internalSCIs.add(new org.infinispan.persistence.rocksdb.PersistenceContextInitializerImpl());
      internalSCIs.add(new org.infinispan.persistence.jdbc.impl.PersistenceContextInitializerImpl());
      internalSCIs.add(new org.infinispan.query.core.stats.impl.PersistenceContextInitializerImpl());
      internalSCIs.add(new org.infinispan.server.core.PersistenceContextInitializerImpl());
   }

   final SerializationContext ctx;

   public Infinispan10Marshaller(Marshaller userMarshaller, List<SerializationContextInitializer> userSCIs) {
      this.ctx = ProtobufUtil.newSerializationContext();

      if (userMarshaller != null) {
         ctx.registerMarshaller(
               new MarshallableUserObject.Marshaller("org.infinispan.persistence.core.MarshallableUserObject", userMarshaller)
         );
      }

      userSCIs.forEach(this::registerInitializer);
      internalSCIs.forEach(this::registerInitializer);
   }

   private void registerInitializer(SerializationContextInitializer initializer) {
      initializer.registerSchema(ctx);
      initializer.registerMarshallers(ctx);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException {
      return unwrapAndInit(ProtobufUtil.fromWrappedByteArray(ctx, buf, offset, length));
   }

   private Object unwrapAndInit(Object o) {
      if (o instanceof MarshallableUserObject) {
         MarshallableUserObject wrapper = (MarshallableUserObject) o;
         return wrapper.get();
      }
      return o;
   }
}
