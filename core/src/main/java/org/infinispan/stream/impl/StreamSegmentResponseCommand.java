package org.infinispan.stream.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.util.IntSet;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.core.UserAwareObjectOutput;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * A stream response command that also returns back suspected segments that need to be retried
 * @param <R> the response type
 */
public class StreamSegmentResponseCommand<R> extends StreamResponseCommand<R> {
   public static final byte COMMAND_ID = 49;

   protected IntSet missedSegments;

   // Only here for CommandIdUniquenessTest
   protected StreamSegmentResponseCommand() { }

   public StreamSegmentResponseCommand(ByteString cacheName) {
      super(cacheName);
   }

   public StreamSegmentResponseCommand(ByteString cacheName, Address origin, Object id, boolean complete, R response,
         IntSet missedSegments) {
      super(cacheName, origin, id, complete, response);
      this.missedSegments = missedSegments;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      csm.receiveResponse(id, getOrigin(), complete, missedSegments, response);
      return CompletableFutures.completedNull();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserAwareObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      output.writeObject(getOrigin());
      output.writeObject(id);
      output.writeBoolean(complete);
      output.writeObject(response);
      output.writeObject(missedSegments);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      setOrigin((Address) input.readObject());
      id = input.readObject();
      complete = input.readBoolean();
      response = (R) input.readObject();
      missedSegments = (IntSet) input.readObject();
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }
}
