package org.infinispan.query.remote.impl.indexing;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.infinispan.commons.CacheException;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.remote.impl.mapping.reference.GlobalReferenceHolder;
import org.infinispan.query.remote.impl.mapping.type.ProtobufKeyValuePair;
import org.infinispan.search.mapper.mapping.EntityConverter;

public final class ProtobufEntityConverter implements EntityConverter {

   private static final PojoRawTypeIdentifier<byte[]> BYTE_ARRAY_TYPE_IDENTIFIER = PojoRawTypeIdentifier.of(byte[].class);
   private static final PojoRawTypeIdentifier<ProtobufKeyValuePair> KEY_VALUE_TYPE_IDENTIFIER = PojoRawTypeIdentifier.of(ProtobufKeyValuePair.class);

   private final SerializationContext serializationContext;
   private final GlobalReferenceHolder globalReferenceHolder;
   private final Set<String> indexedMessageTypes;

   public ProtobufEntityConverter(SerializationContext serializationContext, GlobalReferenceHolder globalReferenceHolder) {
      this.serializationContext = serializationContext;
      this.globalReferenceHolder = globalReferenceHolder;
      this.indexedMessageTypes = globalReferenceHolder.getRootMessages().stream()
            .map(rootMessage -> rootMessage.getFullName()).collect(Collectors.toSet());
   }

   @Override
   public Class<?> targetType() {
      return ProtobufValueWrapper.class;
   }

   @Override
   public Set<PojoRawTypeIdentifier<?>> convertedTypeIdentifiers() {
      return Set.of(BYTE_ARRAY_TYPE_IDENTIFIER, KEY_VALUE_TYPE_IDENTIFIER);
   }

   @Override
   public boolean typeIsIndexed(Class<?> type) {
      return type.equals(targetType());
   }

   @Override
   public ConvertedEntity convert(Object entity, Object providedId) {
      ProtobufValueWrapper valueWrapper = (ProtobufValueWrapper) entity;
      WrappedMessageTagHandler tagHandler = new WrappedMessageTagHandler(valueWrapper, serializationContext);
      try {
         Descriptor wrapperDescriptor = serializationContext.getMessageDescriptor(WrappedMessage.PROTOBUF_TYPE_NAME);
         ProtobufParser.INSTANCE.parse(tagHandler, wrapperDescriptor, valueWrapper.getBinary());
      } catch (IOException e) {
         throw new CacheException(e);
      }

      Descriptor messageDescriptor = valueWrapper.getMessageDescriptor();
      if (messageDescriptor == null) {
         return new ProtobufConvertedEntity(true, null, null);
      }

      String entityName = messageDescriptor.getFullName();
      boolean skip = !indexedMessageTypes.contains(entityName);
      byte[] messageBytes = tagHandler.getMessageBytes();
      if (skip || !globalReferenceHolder.hasKeyMapping(entityName)) {
         return new ProtobufConvertedEntity(skip, entityName, messageBytes);
      }

      ProtobufValueWrapper keyValueWrapper = new ProtobufValueWrapper((byte[]) providedId);
      tagHandler = new WrappedMessageTagHandler(keyValueWrapper, serializationContext);
      try {
         Descriptor wrapperDescriptor = serializationContext.getMessageDescriptor(WrappedMessage.PROTOBUF_TYPE_NAME);
         ProtobufParser.INSTANCE.parse(tagHandler, wrapperDescriptor, keyValueWrapper.getBinary());
      } catch (IOException e) {
         throw new CacheException(e);
      }

      return new ProtobufKeyValueConvertedEntity(skip, entityName, tagHandler.getMessageBytes(), messageBytes);
   }

   private static final class ProtobufConvertedEntity implements ConvertedEntity {
      private final boolean skip;
      private final String entityName;
      private final byte[] value;

      ProtobufConvertedEntity(boolean skip, String entityName, byte[] value) {
         this.skip = skip;
         this.entityName = entityName;
         this.value = value;
      }

      @Override
      public boolean skip() {
         return skip;
      }
      @Override
      public String entityName() {
         return entityName;
      }
      @Override
      public Object value() {
         return value;
      }
   }

   private static class ProtobufKeyValueConvertedEntity implements ConvertedEntity {
      private final boolean skip;
      private final String entityName;
      private final ProtobufKeyValuePair value;

      public ProtobufKeyValueConvertedEntity(boolean skip, String entityName, byte[] key, byte[] value) {
         this.skip = skip;
         this.entityName = entityName;
         this.value = new ProtobufKeyValuePair(key, value);
      }

      @Override
      public boolean skip() {
         return skip;
      }
      @Override
      public String entityName() {
         return entityName;
      }
      @Override
      public Object value() {
         return value;
      }
   }
}
