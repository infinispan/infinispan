package org.infinispan.interceptors;

import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.core.MarshalledValue;

/**
 * A marshalled value interceptor which forces defensive copies to be made
 * proactively. By doing so, clients are no longer able to make any changes
 * via direct object references, so any changes require a cache modification
 * call via put/replace...etc methods.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class DefensiveMarshalledValueInterceptor extends MarshalledValueInterceptor {

   @Override
   protected void compact(MarshalledValue mv) {
      // Force marshalled version to be stored
      if (mv != null)
         mv.compact(true, true);
   }

   @Override
   protected Object processRetVal(Object retVal, InvocationContext ctx) {
      Object ret = retVal;
      if (retVal instanceof MarshalledValue) {
         // Calculate return
         ret = super.processRetVal(ret, ctx);
         // Re-compact in case deserialization happened
         ((MarshalledValue) retVal).compact(true, true);
      }
      return ret;
   }

}
