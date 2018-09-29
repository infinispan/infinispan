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

   private static final boolean trace = log.isTraceEnabled();

   private final Cache cache;

   /**
    * This is lazily initialised in {@link #decodeAndIndex} method. This does not need to be volatile nor do we need to
    * synchronize before accessing it. It may happen to be initialized multiple times by different threads but that is
    * not harmful.
    * <p>
    * The lazy initialization is needed because the cache manager of the cache we receive in our constructor might not
    * have started the ProtobufMetadataManagerImpl's internal cache yet and we do not want to trigger that too early
    * because it can lead to deadlocks so we postpone until the last moment.
    */
   private SerializationContext serializationContext = null;

   /**
    * Lazily initialized in {@link #decodeAndIndex} method, similarly to {@link #serializationContext} field.
    */
   private Descriptor wrapperDescriptor = null;

   public ProtobufValueWrapperFieldBridge(Cache cache) {
      this.cache = cache;
   }

   @Override
   public void set(String name, Object value, Document document, LuceneOptions luceneOptions) {
      if (trace) {
         log.tracef("Setting Lucene document properties for %s in cache %s", value.toString(), cache.getName());
      }
      // This FieldBridge can only be applied to a ProtobufValueWrapper so it's safe to cast here directly
      ProtobufValueWrapper valueWrapper = (ProtobufValueWrapper) value;
      decodeAndIndex(valueWrapper, document, luceneOptions);
   }

   private void decodeAndIndex(ProtobufValueWrapper valueWrapper, Document document, LuceneOptions luceneOptions) {
      if (serializationContext == null) {
         serializationContext = ProtobufMetadataManagerImpl.getSerializationContext(cache.getCacheManager());
         wrapperDescriptor = serializationContext.getMessageDescriptor(WrappedMessage.PROTOBUF_TYPE_NAME);
      }

      try {
         ProtobufParser.INSTANCE.parse(new IndexingWrappedMessageTagHandler(valueWrapper, serializationContext, document, luceneOptions), wrapperDescriptor, valueWrapper.getBinary());
      } catch (IOException e) {
         throw new CacheException(e);
      }
   }
}
