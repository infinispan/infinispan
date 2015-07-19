package org.infinispan.context;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.impl.ClearInvocationContext;
import org.infinispan.remoting.transport.Address;


/**
 * Base class for InvocationContextFactory implementations.
 *
 * @author Mircea Markus
 * @author Dan Berindei
 * @since 7.0
 */
public abstract class AbstractInvocationContextFactory implements InvocationContextFactory {

   protected Configuration config;
   protected Equivalence keyEq;

   // Derived classes must call init() in their @Inject methods, to keep only one @Inject method per class.
   public void init(Configuration config) {
      this.config = config;
      keyEq = config.dataContainer().keyEquivalence();
   }

   @Override
   public InvocationContext createRemoteInvocationContextForCommand(
         VisitableCommand cacheCommand, Address origin) {
      return cacheCommand instanceof ClearCommand ? createClearInvocationContext(origin) :
            createRemoteInvocationContext(origin);
   }

   @Override
   public final InvocationContext createClearNonTxInvocationContext() {
      return createClearInvocationContext(null);
   }

   private ClearInvocationContext createClearInvocationContext(Address origin) {
      ClearInvocationContext context = new ClearInvocationContext(origin);
      return context;
   }
}
