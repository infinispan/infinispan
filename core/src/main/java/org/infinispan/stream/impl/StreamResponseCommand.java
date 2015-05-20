package org.infinispan.stream.impl;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;

import java.util.Collections;
import java.util.UUID;

/**
 * Stream response command used to handle returning intermediate or final responses from the remote node
 * @param <R> the response type
 */
public class StreamResponseCommand<R> extends BaseRpcCommand {
   public static final byte COMMAND_ID = 48;

   protected ClusterStreamManager csm;

   protected UUID id;
   protected boolean complete;
   protected R response;

   // Only here for CommandIdUniquenessTest
   protected StreamResponseCommand() { super(null); }

   public StreamResponseCommand(String cacheName) {
      super(cacheName);
   }

   public StreamResponseCommand(String cacheName, Address origin, UUID id, boolean complete, R response) {
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
   public Object[] getParameters() {
      return new Object[]{getOrigin(), id, complete, response};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      int i = 0;
      setOrigin((Address) parameters[i++]);
      id = (UUID) parameters[i++];
      complete = (Boolean) parameters[i++];
      response = (R) parameters[i++];
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
