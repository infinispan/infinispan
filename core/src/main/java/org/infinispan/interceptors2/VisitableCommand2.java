package org.infinispan.interceptors2;

import org.infinispan.context.InvocationContext;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public interface VisitableCommand2 {
   Object perform(InvocationContext ctx);

   boolean needsExistingValues();
}
