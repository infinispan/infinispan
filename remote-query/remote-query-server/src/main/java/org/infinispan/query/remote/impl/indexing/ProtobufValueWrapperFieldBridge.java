package org.infinispan.query.remote.impl.indexing;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.bridge.LuceneOptions;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.remote.impl.ProtobufMetadataManagerImpl;
import org.infinispan.query.remote.impl.logging.Log;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class ProtobufValueWrapperFieldBridge implements FieldBridge {

   private static final Log log = LogFactory.getLog(ProtobufValueWrapperFieldBridge.class, Log.class);

   private final Cache cache;

   /**
    * This is lazily initialised in {@code decodeAndIndex} method. This does not need to be volatile nor do we need to
    * synchronize before accessing it. It may happen to initialize it multiple times but that is not harmful.
    */
   private SerializationContext serializationContext = null;

   /**
    * Lazily initialized in {@code decodeAndIndex} method, similarly to {@code serializationContext} field.
    */
   private Descriptor wrapperDescriptor = null;

   public ProtobufValueWrapperFieldBridge(Cache cache) {
      this.cache = cache;
   }

   @Override
   public void set(String name, Object value, Document document, LuceneOptions luceneOptions) {
      if (!(value instanceof ProtobufValueWrapper)) {
         throw new IllegalArgumentException("This FieldBridge can only be applied to a " + ProtobufValueWrapper.class.getName());
      }
      ProtobufValueWrapper valueWrapper = (ProtobufValueWrapper) value;
      if (log.isDebugEnabled()) {
         log.debugf("Setting Lucene document properties for %s in cache %s", valueWrapper.toString(), cache.getName());
      }
      decodeAndIndex(valueWrapper, document, luceneOptions);
   }

   private void decodeAndIndex(ProtobufValueWrapper valueWrapper, Document document, LuceneOptions luceneOptions) {
      if (serializationContext == null) {
         serializationContext = ProtobufMetadataManagerImpl.getSerializationContextInternal(cache.getCacheManager());
      }
      if (wrapperDescriptor == null) {
         wrapperDescriptor = serializationContext.getMessageDescriptor(WrappedMessage.PROTOBUF_TYPE_NAME);
      }

      try {
         ProtobufParser.INSTANCE.parse(new WrappedMessageTagHandler(valueWrapper, document, luceneOptions, serializationContext), wrapperDescriptor, valueWrapper.getBinary());
      } catch (IOException e) {
         throw new CacheException(e);
      }
   }
}
