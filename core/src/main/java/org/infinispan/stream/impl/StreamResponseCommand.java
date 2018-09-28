package org.infinispan.stream.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.IntSetsExternalization;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Stream response command used to handle returning intermediate or final responses from the remote node
 * @param <R> the response type
 */
public class StreamResponseCommand<R> extends BaseRpcCommand {
   public static final byte COMMAND_ID = 48;

   @Inject protected ClusterStreamManager csm;

   protected Object id;
   protected boolean complete;
   protected R response;
   protected IntSet missedSegments;

   // Only here for CommandIdUniquenessTest
   protected StreamResponseCommand() { super(null); }

   public StreamResponseCommand(ByteString cacheName) {
      super(cacheName);
   }

   public StreamResponseCommand(ByteString cacheName, Address origin, Object id, boolean complete, R response) {
      this(cacheName, origin, id, complete, IntSets.immutableEmptySet(), response);
   }
   public StreamResponseCommand(ByteString cacheName, Address origin, Object id, boolean complete,
                                IntSet missedSegments, R response) {
      super(cacheName);
      setOrigin(origin);
      this.id = id;
      this.complete = complete;
      this.response = response;
      this.missedSegments = missedSegments;
   }

   public void inject(ClusterStreamManager csm) {
      this.csm = csm;
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
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(getOrigin());
      output.writeObject(id);
      output.writeBoolean(complete);
      IntSetsExternalization.writeTo(output, missedSegments);
      output.writeObject(response);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      setOrigin((Address) input.readObject());
      id = input.readObject();
      complete = input.readBoolean();
      missedSegments = IntSetsExternalization.readFrom(input);
      response = (R) input.readObject();
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("StreamResponseCommand{");
      sb.append("id=").append(id);
      sb.append(", complete=").append(complete);
      sb.append(", missedSegments=").append(missedSegments);
      sb.append(", response=").append(response);
      sb.append('}');
      return sb.toString();
   }
}
