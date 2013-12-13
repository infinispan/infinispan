package org.infinispan.context;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Start;
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
   }

   @Start
   public void start() {
      keyEq = config.dataContainer().keyEquivalence();
   }

   @Override
   public InvocationContext createRemoteInvocationContextForCommand(
         VisitableCommand cacheCommand, Address origin) {
      return createRemoteInvocationContext(origin);
   }
}
