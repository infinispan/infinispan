package org.infinispan.interceptors.base;

import org.infinispan.Cache;
import org.infinispan.cache.impl.CacheImpl;
import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This is the base class for all interceptors to extend, and implements the {@link Visitor} interface allowing it to
 * intercept invocations on {@link VisitableCommand}s.
 * <p/>
 * Commands are either created by the {@link CacheImpl} (for invocations on the {@link Cache} public interface), or
 * by the {@link org.infinispan.remoting.inboundhandler.InboundInvocationHandler} for remotely originating invocations, and are passed up the interceptor chain
 * by using the {@link InterceptorChain} helper class.
 * <p/>
 * When writing interceptors, authors can either override a specific visitXXX() method (such as {@link
 * #visitGetKeyValueCommand(InvocationContext, GetKeyValueCommand)}) or the more generic {@link
 * #handleDefault(InvocationContext, VisitableCommand)} which is the default behaviour of any visit method, as defined
 * in {@link AbstractVisitor#handleDefault(InvocationContext, VisitableCommand)}.
 * <p/>
 * The preferred approach is to override the specific visitXXX() methods that are of interest rather than to override
 * {@link #handleDefault(InvocationContext, VisitableCommand)} and then write a series of if statements or a switch
 * block, if command-specific behaviour is needed.
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @see VisitableCommand
 * @see Visitor
 * @see InterceptorChain
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public abstract class CommandInterceptor extends AbstractVisitor {

   private CommandInterceptor next;

   protected Configuration cacheConfiguration;

   private static final Log log = LogFactory.getLog(CommandInterceptor.class);
   
   protected Log getLog() {
      return log;
   }

   @Inject
   public void injectConfiguration(Configuration configuration) {
      this.cacheConfiguration = configuration;
   }

   /**
    * Retrieves the next interceptor in the chain.
    *
    * @return the next interceptor in the chain.
    */
   public final CommandInterceptor getNext() {
      return next;
   }

   /**
    * @return true if there is another interceptor in the chain after this; false otherwise.
    */
   public final boolean hasNext() {
      return getNext() != null;
   }

   /**
    * Sets the next interceptor in the chain to the interceptor passed in.
    *
    * @param next next interceptor in the chain.
    */
   public final void setNext(CommandInterceptor next) {
      this.next = next;
   }

   /**
    * Invokes the next interceptor in the chain.  This is how interceptor implementations should pass a call up the
    * chain to the next interceptor.
    *
    * @param ctx     invocation context
    * @param command command to pass up the chain.
    * @return return value of the invocation
    * @throws Throwable in the event of problems
    */
   public final Object invokeNextInterceptor(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return command.acceptVisitor(ctx, next);
   }

   /**
    * The default behaviour of the visitXXX methods, which is to ignore the call and pass the call up to the next
    * interceptor in the chain.
    *
    * @param ctx     invocation context
    * @param command command to invoke
    * @return return value
    * @throws Throwable in the event of problems
    */
   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return invokeNextInterceptor(ctx, command);
   }

   protected final long getLockAcquisitionTimeout(LocalFlagAffectedCommand command, boolean skipLocking) {
      if (!skipLocking)
         return command.hasFlag(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT) ?
               0 : cacheConfiguration.locking().lockAcquisitionTimeout();

      return -1;
   }

   protected final boolean hasSkipLocking(LocalFlagAffectedCommand command) {
      return command.hasFlag(Flag.SKIP_LOCKING);
   }

}
