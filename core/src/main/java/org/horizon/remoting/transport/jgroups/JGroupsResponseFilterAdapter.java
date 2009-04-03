package org.horizon.remoting.transport.jgroups;

import org.horizon.remoting.ResponseFilter;
import org.jgroups.Address;
import org.jgroups.blocks.RspFilter;

/**
 * Acts as a bridge between JGroups RspFilter and {@link org.horizon.remoting.ResponseFilter}.
 *
 * @author Manik Surtani
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
      return r.isAcceptable(response, new JGroupsAddress(sender));
   }

   public boolean needMoreResponses() {
      return r.needMoreResponses();
   }
}
