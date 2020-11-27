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
import org.infinispan.search.mapper.mapping.EntityConverter;

public class ProtobufEntityConverter implements EntityConverter {

   private final static PojoRawTypeIdentifier<byte[]> BYTE_ARRAY_TYPE_IDENTIFIER = PojoRawTypeIdentifier.of(byte[].class);

   private final SerializationContext serializationContext;
   private final Set<String> indexedMessageTypes;

   public ProtobufEntityConverter(SerializationContext serializationContext, Set<GlobalReferenceHolder.RootMessageInfo> rootMessages) {
      this.serializationContext = serializationContext;
      this.indexedMessageTypes = rootMessages.stream().map(rootMessage -> rootMessage.getFullName()).collect(Collectors.toSet());
   }

   @Override
   public Class<?> targetType() {
      return ProtobufValueWrapper.class;
   }

   @Override
   public PojoRawTypeIdentifier<?> convertedTypeIdentifier() {
      return BYTE_ARRAY_TYPE_IDENTIFIER;
   }

   @Override
   public ConvertedEntity convert(Object entity) {
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
      return new ProtobufConvertedEntity(skip, entityName, messageBytes);
   }

   private class ProtobufConvertedEntity implements ConvertedEntity {
      private final boolean skip;
      private final String entityName;
      private final byte[] value;

      public ProtobufConvertedEntity(boolean skip, String entityName, byte[] value) {
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

}
