package org.infinispan.commands.write;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableThrowable;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.ResponseCollectors;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;

/**
 * A command that represents an exception acknowledge sent by any owner.
 * <p>
 * The acknowledge represents an unsuccessful execution of the operation.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@ProtoTypeId(ProtoStreamTypeIds.EXCEPTION_ACK_COMMAND)
public class ExceptionAckCommand extends BaseRpcCommand {
   public static final byte COMMAND_ID = 42;

   @ProtoField(number = 2, defaultValue = "-1")
   final long id;

   @ProtoField(number = 3, defaultValue = "-1")
   final int topologyId;

   private Throwable throwable;

   public ExceptionAckCommand(ByteString cacheName, long id, Throwable throwable, int topologyId) {
      super(cacheName);
      this.id = id;
      this.topologyId = topologyId;
      this.throwable = throwable;
   }

   @ProtoFactory
   ExceptionAckCommand(ByteString cacheName, long id, int topologyId, MarshallableThrowable throwable) {
      this(cacheName, id, throwable.get(), topologyId);
   }

   @ProtoField(number = 4)
   MarshallableThrowable getThrowable() {
      return MarshallableThrowable.create(throwable);
   }

   public void ack(CommandAckCollector ackCollector) {
      CacheException remoteException = ResponseCollectors.wrapRemoteException(getOrigin(), throwable);
      ackCollector.completeExceptionally(id, remoteException, topologyId);
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public String toString() {
      return "ExceptionAckCommand{" +
            "id=" + id +
            ", throwable=" + throwable +
            ", topologyId=" + topologyId +
            '}';
   }
}
