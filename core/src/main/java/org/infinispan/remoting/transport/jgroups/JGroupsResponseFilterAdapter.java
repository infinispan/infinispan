package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.blocks.RspFilter;

/**
 * Acts as a bridge between JGroups RspFilter and {@link org.infinispan.remoting.rpc.ResponseFilter}.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
public final class JGroupsResponseFilterAdapter implements RspFilter {
   private static final Log log = LogFactory.getLog(JGroupsResponseFilterAdapter.class);

   final ResponseFilter r;

   /**
    * Creates an instance of the adapter
    *
    * @param r response filter to wrap
    */
   public JGroupsResponseFilterAdapter(ResponseFilter r) {
      this.r = r;
   }

   @Override
   public boolean isAcceptable(Object response, Address sender) {
      try {
         if (response instanceof Exception)
            response = new ExceptionResponse((Exception) response);
         else if (response instanceof Throwable)
            response = new ExceptionResponse(new RuntimeException((Throwable) response));

         return r.isAcceptable((Response) response, JGroupsTransport.fromJGroupsAddress(sender));
      } catch (Throwable t) {
         log.error("Exception in response filter: ", t);
         throw t;
      }
   }

   @Override
   public boolean needMoreResponses() {
      return r.needMoreResponses();
   }
}
