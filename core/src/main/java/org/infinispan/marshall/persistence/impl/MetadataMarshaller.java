package org.infinispan.marshall.persistence.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.CacheException;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.functional.MetaParam;
import org.infinispan.functional.impl.MetaParams;
import org.infinispan.functional.impl.MetaParamsInternalMetadata;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.EnumMarshaller;
import org.infinispan.protostream.MessageMarshaller;

final class MetadataMarshaller implements MessageMarshaller<Metadata> {

   private static final Map<Class, Type> typeMap = new HashMap<>();
   static {
      typeMap.put(EmbeddedMetadata.class, Type.IMMORTAL);
      typeMap.put(EmbeddedMetadata.EmbeddedExpirableMetadata.class, Type.EXPIRABLE);
      typeMap.put(EmbeddedMetadata.EmbeddedLifespanExpirableMetadata.class, Type.LIFESPAN_EXPIRABLE);
      typeMap.put(EmbeddedMetadata.EmbeddedMaxIdleExpirableMetadata.class, Type.MAXIDLE_EXPIRABLE);
      typeMap.put(MetaParamsInternalMetadata.class, Type.META_PARAM);
   }

   public enum Type {
      IMMORTAL(0),
      EXPIRABLE(1),
      LIFESPAN_EXPIRABLE(2),
      MAXIDLE_EXPIRABLE(3),
      META_PARAM(4);

      final int index;

      Type(int index) {
         this.index = index;
      }

      static Type get(int index) {
         for (Type type : Type.values())
            if (type.index == index)
               return type;
         return null;
      }
   }


   @Override
   public Metadata readFrom(ProtoStreamReader reader) throws IOException {
      Type type = reader.readEnum("type", Type.class);
      if (type == null)
         return new EmbeddedMetadata.Builder().build();

      if (type == Type.META_PARAM) {
         return createMetaParamsMetadata(reader);
      }

      Metadata.Builder builder = new EmbeddedMetadata.Builder();
      builder.version(reader.readObject("version", EntryVersion.class));
      switch (type) {
         case EXPIRABLE:
            builder.lifespan(reader.readLong("lifespan"));
            builder.maxIdle(reader.readLong("maxIdle"));
            break;
         case LIFESPAN_EXPIRABLE:
            builder.lifespan(reader.readLong("lifespan"));
            break;
         case MAXIDLE_EXPIRABLE:
            builder.maxIdle(reader.readLong("maxIdle"));
            break;
      }
      return builder.build();
   }

   private Metadata createMetaParamsMetadata(ProtoStreamReader reader) throws IOException {
      EntryVersion version = reader.readObject("version", EntryVersion.class);
      Long lifespan = reader.readLong("lifespan");
      Long maxIdle = reader.readLong("maxIdle");
      MetaParams params = new MetaParams();
      if (version != null)
         params.add(new MetaParam.MetaEntryVersion(version));

      if (lifespan != null)
         params.add(new MetaParam.MetaLifespan(lifespan));

      if (maxIdle != null)
         params.add(new MetaParam.MetaMaxIdle(maxIdle));

      return MetaParamsInternalMetadata.from(params);
   }

   @Override
   public void writeTo(ProtoStreamWriter writer, Metadata metadata) throws IOException {
      Type type = typeMap.get(metadata.getClass());
      if (type == null)
         return;

      writer.writeEnum("type", type, Type.class);

      if (type == Type.META_PARAM) {
         MetaParamsInternalMetadata m = (MetaParamsInternalMetadata) metadata;
         m.findMetaParam(MetaParam.MetaEntryVersion.class).ifPresent(v -> {
            try {
               writer.writeObject("version", v.get(), EntryVersion.class);
            } catch (IOException e) {
               throw new CacheException(e);
            }
         });
         m.findMetaParam(MetaParam.MetaLifespan.class).ifPresent(v -> writeMetaParamLong(writer, "lifespan", v.get()));
         m.findMetaParam(MetaParam.MetaMaxIdle.class).ifPresent(v -> writeMetaParamLong(writer, "maxIdle", v.get()));
         return;
      }

      writer.writeObject("version", metadata.version(), EntryVersion.class);
      switch (type) {
         case EXPIRABLE:
            writer.writeLong("lifespan", metadata.lifespan());
            writer.writeLong("maxIdle", metadata.maxIdle());
            break;
         case LIFESPAN_EXPIRABLE:
            writer.writeLong("lifespan", metadata.lifespan());
            break;
         case MAXIDLE_EXPIRABLE:
            writer.writeLong("maxIdle", metadata.maxIdle());
            break;
      }
   }

   private void writeMetaParamLong(ProtoStreamWriter writer, String fieldName, long value) {
      try {
         writer.writeLong(fieldName, value);
      } catch (IOException e) {
         throw new CacheException(e);
      }
   }

   @Override
   public Class<Metadata> getJavaClass() {
      return Metadata.class;
   }

   @Override
   public String getTypeName() {
      return "persistence.Metadata";
   }

   public static class TypeMarshaller implements EnumMarshaller<Type> {
      @Override
      public Type decode(int enumValue) {
         return Type.get(enumValue);
      }

      @Override
      public int encode(Type metadataType) throws IllegalArgumentException {
         return metadataType.index;
      }

      @Override
      public Class<Type> getJavaClass() {
         return Type.class;
      }

      @Override
      public String getTypeName() {
         return "persistence.Metadata.Type";
      }
   }
}
