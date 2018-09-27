package org.infinispan.query.remote.impl.indexing;

import java.io.IOException;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;

/**
 * Intercepts write operations to the cache where the value is a {@link ProtobufValueWrapper} object and parses the
 * underlying WrappedMessage to discover the actual user message type. Knowledge of the message type is needed in order
 * to be able to tell later in the interceptor chain if this values gets to be indexed or not.
 *
 * @author anistor@redhat.com
 * @since 9.3
 */
public final class ProtobufValueWrapperInterceptor extends DDAsyncInterceptor {

   private final SerializationContext serializationContext;

   private final Descriptor wrapperDescriptor;

   public ProtobufValueWrapperInterceptor(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
      wrapperDescriptor = serializationContext.getMessageDescriptor(WrappedMessage.PROTOBUF_TYPE_NAME);
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

   /**
    * Discovers the type of the protobuf payload and if it is a message type it sets the descriptor using {@link
    * ProtobufValueWrapper#setMessageDescriptor}.
    *
    * @param valueWrapper the wrapper of the protobuf binary payload
    */
   private void discoverMessageType(ProtobufValueWrapper valueWrapper) {
      try {
         ProtobufParser.INSTANCE.parse(new WrappedMessageTagHandler(valueWrapper, serializationContext), wrapperDescriptor, valueWrapper.getBinary());
      } catch (IOException e) {
         throw new CacheException(e);
      }
   }
}
