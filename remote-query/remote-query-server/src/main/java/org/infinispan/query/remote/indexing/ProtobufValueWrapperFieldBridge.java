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
      SerializationContext serCtx = ProtobufMetadataManager.getSerializationContext(cache.getCacheManager());
      Descriptors.Descriptor wrapperDescriptor = serCtx.getMessageDescriptor(WrappedMessage.PROTOBUF_TYPE_NAME);
      try {
         new ProtobufParser().parse(new WrappedMessageTagHandler(document, luceneOptions, serCtx), wrapperDescriptor, bytes);
      } catch (IOException e) {
         throw new CacheException(e);
      }
   }
}
