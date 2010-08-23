package org.infinispan.interceptors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

/**
 * Interceptor in charge of eager, implicit locking of cache keys across cluster within transactional context
 * <p/>
 * <p/>
 * For more details refer to: https://jira.jboss.org/jira/browse/ISPN-70 https://jira.jboss.org/jira/browse/ISPN-48
 *
 * @author <a href="mailto:vblagoje@redhat.com">Vladimir Blagojevic (vblagoje@redhat.com)</a>
 * @since 4.0
 */
public class ImplicitEagerLockingInterceptor extends CommandInterceptor {

   private CommandsFactory cf;

   @Inject
   public void init(CommandsFactory factory) {
      this.cf = factory;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      boolean localTxScope = ctx.isInTxScope() & ctx.isOriginLocal();
      if (localTxScope) {
         lockEagerly(ctx, Collections.singleton(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      boolean localTxScope = ctx.isInTxScope() & ctx.isOriginLocal();
      if (localTxScope) {
         lockEagerly(ctx, Collections.singleton(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command)
         throws Throwable {
      boolean localTxScope = ctx.isInTxScope() & ctx.isOriginLocal();
      if (localTxScope) {
         lockEagerly(ctx, Collections.singleton(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      boolean localTxScope = ctx.isInTxScope() & ctx.isOriginLocal();
      if (localTxScope) {
         lockEagerly(ctx, new HashSet<Object>(command.getMap().keySet()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      boolean localTxScope = ctx.isInTxScope() & ctx.isOriginLocal();
      if (localTxScope) {
         lockEagerly(ctx, Collections.singleton(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command)
         throws Throwable {
      boolean localTxScope = ctx.isInTxScope() & ctx.isOriginLocal();
      if (localTxScope) {
         lockEagerly(ctx, Collections.singleton(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   private Object lockEagerly(InvocationContext ctx, Collection<Object> keys) throws Throwable {
      LockControlCommand lcc = cf.buildLockControlCommand(keys, true);
      return invokeNextInterceptor(ctx, lcc);
   }
}
