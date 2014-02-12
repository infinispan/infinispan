package org.infinispan.query.remote.indexing;

import com.google.protobuf.Descriptors;
import org.apache.lucene.document.Document;
import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.bridge.LuceneOptions;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.query.remote.ProtobufMetadataManager;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class ProtobufValueWrapperFieldBridge implements FieldBridge {

   private final Cache cache;

   /**
    * This is lazily initialised in {@code decodeAndIndex} method. This does not need to be volatile nor do we need to
    * synchronize before accessing it. It may happen to initialize it multiple times but that is not harmful.
    */
   private SerializationContext serializationContext = null;

   /**
    * Lazily initialized in {@code decodeAndIndex} method, similarly to {@code serializationContext} field.
    */
   private Descriptors.Descriptor wrapperDescriptor = null;

   public ProtobufValueWrapperFieldBridge(Cache cache) {
      this.cache = cache;
   }

   @Override
   public void set(String name, Object value, Document document, LuceneOptions luceneOptions) {
      if (!(value instanceof ProtobufValueWrapper)) {
         throw new IllegalArgumentException("This FieldBridge can only be applied to a ProtobufValueWrapper");
      }
      ProtobufValueWrapper valueWrapper = (ProtobufValueWrapper) value;

      decodeAndIndex(valueWrapper.getBinary(), document, luceneOptions);
   }

   private void decodeAndIndex(byte[] bytes, Document document, LuceneOptions luceneOptions) {
      if (serializationContext == null) {
         serializationContext = ProtobufMetadataManager.getSerializationContext(cache.getCacheManager());
      }
      if (wrapperDescriptor == null) {
         wrapperDescriptor = serializationContext.getMessageDescriptor(WrappedMessage.PROTOBUF_TYPE_NAME);
      }

      try {
         ProtobufParser.INSTANCE.parse(new WrappedMessageTagHandler(document, luceneOptions, serializationContext), wrapperDescriptor, bytes);
      } catch (IOException e) {
         throw new CacheException(e);
      }
   }
}
