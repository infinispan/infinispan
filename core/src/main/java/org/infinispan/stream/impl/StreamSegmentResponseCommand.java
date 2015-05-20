package org.infinispan.stream.impl;

import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Address;

import java.util.Set;
import java.util.UUID;

/**
 * A stream response command taht also returns back suspected segments that need to be retried
 * @param <R> the response type
 */
public class StreamSegmentResponseCommand<R> extends StreamResponseCommand<R> {
   public static final byte COMMAND_ID = 49;

   protected Set<Integer> missedSegments;

   // Only here for CommandIdUniquenessTest
   protected StreamSegmentResponseCommand() { }

   public StreamSegmentResponseCommand(String cacheName) {
      super(cacheName);
   }

   public StreamSegmentResponseCommand(String cacheName, Address origin, UUID id, boolean complete, R response,
                                       Set<Integer> missedSegments) {
      super(cacheName, origin, id, complete, response);
      this.missedSegments = missedSegments;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      csm.receiveResponse(id, getOrigin(), complete, missedSegments, response);
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{getOrigin(), id, complete, response, missedSegments};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      int i = 0;
      setOrigin((Address) parameters[i++]);
      id = (UUID) parameters[i++];
      complete = (Boolean) parameters[i++];
      response = (R) parameters[i++];
      missedSegments = (Set<Integer>) parameters[i++];
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
