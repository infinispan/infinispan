package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.jgroups.Address;
import org.jgroups.blocks.RspFilter;

/**
 * Acts as a bridge between JGroups RspFilter and {@link org.infinispan.remoting.rpc.ResponseFilter}.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
public class JGroupsResponseFilterAdapter implements RspFilter {

   ResponseFilter r;

   /**
    * Creates an instance of the adapter
    *
    * @param r response filter to wrap
    */
   public JGroupsResponseFilterAdapter(ResponseFilter r) {
      this.r = r;
   }

   public boolean isAcceptable(Object response, Address sender) {
      if (response instanceof Exception)
         response = new ExceptionResponse((Exception) response);
      else if (response instanceof Throwable)
         response = new ExceptionResponse(new RuntimeException((Throwable)response));

      return r.isAcceptable((Response) response, new JGroupsAddress(sender));
   }

   public boolean needMoreResponses() {
      return r.needMoreResponses();
   }
}
