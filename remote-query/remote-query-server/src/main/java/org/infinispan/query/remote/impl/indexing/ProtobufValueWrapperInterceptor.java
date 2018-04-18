package org.infinispan.query.remote.impl.indexing;

import java.io.IOException;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.remote.impl.ProtobufMetadataManagerImpl;

/**
 * Intercepts write operations to the cache where the value is a ProtobufValueWrapper object and parses the underlying
 * WrappedMessage to discover the inner message type. The message type is needed in order to be able to tell later in
 * the interceptor chain if it gets indexed or not.
 *
 * @author anistor@redhat.com
 * @since 9.3
 */
public final class ProtobufValueWrapperInterceptor extends DDAsyncInterceptor {

   private final EmbeddedCacheManager cacheManager;

   /**
    * This is lazily initialised in {@code discoverMessageType} method. This does not need to be volatile nor do we need
    * to synchronize before accessing it. It may happen to initialize it multiple times but that is not harmful.
    */
   private SerializationContext serializationContext = null;

   /**
    * Lazily initialized in {@code discoverMessageType} method, similarly to {@code serializationContext} field.
    */
   private Descriptor wrapperDescriptor = null;

   public ProtobufValueWrapperInterceptor(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
      intercept(command.getValue());
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
      for (Object value : command.getMap().values()) {
         intercept(value);
      }
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      intercept(command.getOldValue());
      intercept(command.getNewValue());
      return super.visitReplaceCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      intercept(command.getValue());
      return super.visitRemoveCommand(ctx, command);
   }

   private void intercept(Object value) {
      if (value instanceof ProtobufValueWrapper) {
         discoverMessageType((ProtobufValueWrapper) value);
      }
   }

   private void discoverMessageType(ProtobufValueWrapper valueWrapper) {
      if (serializationContext == null) {
         serializationContext = ProtobufMetadataManagerImpl.getSerializationContextInternal(cacheManager);
      }
      if (wrapperDescriptor == null) {
         wrapperDescriptor = serializationContext.getMessageDescriptor(WrappedMessage.PROTOBUF_TYPE_NAME);
      }

      try {
         ProtobufParser.INSTANCE.parse(new WrappedMessageTagHandler(valueWrapper, serializationContext), wrapperDescriptor, valueWrapper.getBinary());
      } catch (IOException e) {
         throw new CacheException(e);
      }
   }
}
