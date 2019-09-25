package org.infinispan.commons.marshall;

import java.io.IOException;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.WrappedMessageTypeIdMapper;
import org.infinispan.protostream.config.Configuration;

/**
 * Provides the starting point for implementing a {@link org.infinispan.commons.marshall.Marshaller} that uses Protobuf
 * encoding.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public class ProtoStreamMarshaller extends AbstractMarshaller {

   private final SerializationContext serializationContext;

   public ProtoStreamMarshaller() {
      this(ProtobufUtil.newSerializationContext(
            Configuration.builder()
                  .wrappingConfig()
                  .wrappedMessageTypeIdMapper(getLegacyWrappedMessageTypeIdMapper())
                  .build()
            )
      );
   }

   public ProtoStreamMarshaller(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
   }

   public void register(SerializationContextInitializer initializer) {
      initializer.registerSchema(serializationContext);
      initializer.registerMarshallers(serializationContext);
   }

   /**
    * @return the SerializationContext instance
    */
   public SerializationContext getSerializationContext() {
      return serializationContext;
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      return ProtobufUtil.fromWrappedByteArray(getSerializationContext(), buf, offset, length);
   }

   @Override
   public boolean isMarshallable(Object o) {
      // our marshaller can handle all of these primitive/scalar types as well even if we do not
      // have a per-type marshaller defined in our SerializationContext
      return o instanceof String ||
            o instanceof Long ||
            o instanceof Integer ||
            o instanceof Double ||
            o instanceof Float ||
            o instanceof Boolean ||
            o instanceof byte[] ||
            o instanceof Byte ||
            o instanceof Short ||
            o instanceof Character ||
            o instanceof java.util.Date ||
            o instanceof java.time.Instant ||
            getSerializationContext().canMarshall(o.getClass());
   }

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException, InterruptedException {
      byte[] bytes = ProtobufUtil.toWrappedByteArray(getSerializationContext(), o);
      return new ByteBufferImpl(bytes, 0, bytes.length);
   }

   @Override
   public MediaType mediaType() {
      return MediaType.APPLICATION_PROTOSTREAM;
   }

   /**
    * A type id mapper that maps old and new type ids back and forth.
    */
   public static WrappedMessageTypeIdMapper getLegacyWrappedMessageTypeIdMapper() {
      // to support the 9.x legacy, the values starting at 1000000 were used for WrappedMessage and remote query objects
      // and need to be mapped to the new 0..65535 range
      return new WrappedMessageTypeIdMapper() {

         private static final int LEGACY_WRAPPED_MESSAGE = 1000000;
         private static final int LEGACY_REMOTE_QUERY_REQUEST = 1000101;
         private static final int LEGACY_REMOTE_QUERY_RESPONSE = 1000102;
         private static final int LEGACY_ICKLE_FILTER_RESULT = 1000103;
         private static final int LEGACY_ICKLE_CONTINUOUS_QUERY_RESULT = 1000104;

         @Override
         public int mapTypeIdIn(int typeId, ImmutableSerializationContext ctx) {
            switch (typeId) {
               // historically, the values above 1000000 were used for internal stuff
               // mapping them unconditionally is OK
               case LEGACY_WRAPPED_MESSAGE:
                  return ProtoStreamTypeIds.WRAPPED_MESSAGE;
               case LEGACY_REMOTE_QUERY_REQUEST:
                  return ProtoStreamTypeIds.REMOTE_QUERY_REQUEST;
               case LEGACY_REMOTE_QUERY_RESPONSE:
                  return ProtoStreamTypeIds.REMOTE_QUERY_RESPONSE;
               case LEGACY_ICKLE_FILTER_RESULT:
                  return ProtoStreamTypeIds.ICKLE_FILTER_RESULT;
               case LEGACY_ICKLE_CONTINUOUS_QUERY_RESULT:
                  return ProtoStreamTypeIds.ICKLE_CONTINUOUS_QUERY_RESULT;
            }
            return typeId;
         }

         @Override
         public int mapTypeIdOut(int typeId, ImmutableSerializationContext ctx) {
            // when writing back we map the new ids back to the legacy ones
            // TODO [anistor] this has to happen conditionally: only if new server + old client or old server + new client, where 'new' means HR version >= 3.0
            switch (typeId) {
               case ProtoStreamTypeIds.WRAPPED_MESSAGE:
                  return LEGACY_WRAPPED_MESSAGE;
               case ProtoStreamTypeIds.REMOTE_QUERY_REQUEST:
                  return LEGACY_REMOTE_QUERY_REQUEST;
               case ProtoStreamTypeIds.REMOTE_QUERY_RESPONSE:
                  return LEGACY_REMOTE_QUERY_RESPONSE;
               case ProtoStreamTypeIds.ICKLE_FILTER_RESULT:
                  return LEGACY_ICKLE_FILTER_RESULT;
               case ProtoStreamTypeIds.ICKLE_CONTINUOUS_QUERY_RESULT:
                  return LEGACY_ICKLE_CONTINUOUS_QUERY_RESULT;
            }
            return typeId;
         }
      };
   }
}
