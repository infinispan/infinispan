package org.infinispan.stream.impl;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;

/**
 * Stream response command used to handle returning intermediate or final responses from the remote node
 * @param <R> the response type
 */
public class StreamResponseCommand<R> extends BaseRpcCommand {
   public static final byte COMMAND_ID = 48;

   protected ClusterStreamManager csm;

   protected Object id;
   protected boolean complete;
   protected R response;

   // Only here for CommandIdUniquenessTest
   protected StreamResponseCommand() { super(null); }

   public StreamResponseCommand(String cacheName) {
      super(cacheName);
   }

   public StreamResponseCommand(String cacheName, Address origin, Object id, boolean complete, R response) {
      super(cacheName);
      setOrigin(origin);
      this.id = id;
      this.complete = complete;
      this.response = response;
   }

   @Inject
   public void inject(ClusterStreamManager csm) {
      this.csm = csm;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      csm.receiveResponse(id, getOrigin(), complete, Collections.emptySet(), response);
      return null;
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
      output.writeObject(response);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      setOrigin((Address) input.readObject());
      id = input.readObject();
      complete = input.readBoolean();
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
}
