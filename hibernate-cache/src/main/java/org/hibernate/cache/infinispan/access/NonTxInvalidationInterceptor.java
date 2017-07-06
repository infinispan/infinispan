/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.cache.infinispan.access;

import org.hibernate.cache.infinispan.util.CacheCommandInitializer;
import org.hibernate.cache.infinispan.util.InfinispanMessageLogger;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InvocationFinallyFunction;
import org.infinispan.interceptors.impl.InvalidationInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;

/**
 * This interceptor should completely replace default InvalidationInterceptor.
 * We need to send custom invalidation commands with transaction identifier (as the invalidation)
 * since we have to do a two-phase invalidation (releasing the locks as JTA synchronization),
 * although the cache itself is non-transactional.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
@MBean(objectName = "Invalidation", description = "Component responsible for invalidating entries on remote caches when entries are written to locally.")
public class NonTxInvalidationInterceptor extends BaseInvalidationInterceptor {
	private final PutFromLoadValidator putFromLoadValidator;
	private CacheCommandInitializer commandInitializer;

	private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(InvalidationInterceptor.class);
   private static final Log ispnLog = LogFactory.getLog(NonTxInvalidationInterceptor.class);

	public NonTxInvalidationInterceptor(PutFromLoadValidator putFromLoadValidator) {
		this.putFromLoadValidator = putFromLoadValidator;
	}

	@Inject
	public void injectDependencies(CacheCommandInitializer commandInitializer) {
		this.commandInitializer = commandInitializer;
	}

	@Override
	public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
		if (command.hasFlag(Flag.PUT_FOR_EXTERNAL_READ)) {
			return invokeNext(ctx, command);
		}
		else {
			boolean isTransactional = putFromLoadValidator.registerRemoteInvalidation(command.getKey(), command.getKeyLockOwner());
			if (!isTransactional) {
				throw new IllegalStateException("Put executed without transaction!");
			}
			if (!putFromLoadValidator.beginInvalidatingWithPFER(command.getKeyLockOwner(), command.getKey(), command.getValue())) {
				log.failedInvalidatePendingPut(command.getKey(), cacheName);
			}
         RemoveCommand removeCommand = commandsFactory.buildRemoveCommand(command.getKey(), null, command.getFlagsBitSet());
         return invokeNextAndHandle( ctx, removeCommand, new InvalidateAndReturnFunction(isTransactional, command.getKeyLockOwner()) );
		}
	}

	@Override
	public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
		throw new UnsupportedOperationException("Unexpected replace");
	}

	@Override
	public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
		boolean isTransactional = putFromLoadValidator.registerRemoteInvalidation(command.getKey(), command.getKeyLockOwner());
		if (isTransactional) {
			if (!putFromLoadValidator.beginInvalidatingKey(command.getKeyLockOwner(), command.getKey())) {
				log.failedInvalidatePendingPut(command.getKey(), cacheName);
			}
		}
		else {
			log.trace("This is an eviction, not invalidating anything");
		}
      return invokeNextAndHandle( ctx, command, new InvalidateAndReturnFunction(isTransactional, command.getKeyLockOwner()) );
	}

	@Override
	public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
		Object retval = invokeNext(ctx, command);
		if (!isLocalModeForced(command)) {
			// just broadcast the clear command - this is simplest!
			if (ctx.isOriginLocal()) {
				rpcManager.invokeRemotely(getMembers(), command, isSynchronous(command) ? syncRpcOptions : asyncRpcOptions);
			}
		}
		return retval;
	}

	@Override
	public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
		throw new UnsupportedOperationException("Unexpected putAll");
	}

	private <T extends WriteCommand & RemoteLockCommand> void invalidateAcrossCluster(
			T command, boolean isTransactional, Object key, Object keyLockOwner) throws Throwable {
		// increment invalidations counter if statistics maintained
		incrementInvalidations();
		InvalidateCommand invalidateCommand;
		if (!isLocalModeForced(command)) {
			if (isTransactional) {
				invalidateCommand = commandInitializer.buildBeginInvalidationCommand(
                  EnumUtil.EMPTY_BIT_SET, new Object[] { key }, keyLockOwner);
			}
			else {
            invalidateCommand = commandsFactory.buildInvalidateCommand(EnumUtil.EMPTY_BIT_SET, new Object[] {key });
			}
			if (log.isDebugEnabled()) {
				log.debug("Cache [" + rpcManager.getAddress() + "] replicating " + invalidateCommand);
			}

			rpcManager.invokeRemotely(getMembers(), invalidateCommand, isSynchronous(command) ? syncRpcOptions : asyncRpcOptions);
		}
	}

   @Override
   protected Log getLog() {
      return ispnLog;
   }

   class InvalidateAndReturnFunction implements InvocationFinallyFunction {

      final boolean isTransactional;
      final Object keyLockOwner;

      InvalidateAndReturnFunction(boolean isTransactional, Object keyLockOwner) {
         this.isTransactional = isTransactional;
         this.keyLockOwner = keyLockOwner;
      }

      @Override
      public Object apply(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable throwable)
            throws Throwable {
         RemoveCommand removeCmd = (RemoveCommand) rCommand;
         if ( removeCmd.isSuccessful()) {
            invalidateAcrossCluster(removeCmd, isTransactional, removeCmd.getKey(), keyLockOwner);
         }
         return rv;
      }

   }

}
