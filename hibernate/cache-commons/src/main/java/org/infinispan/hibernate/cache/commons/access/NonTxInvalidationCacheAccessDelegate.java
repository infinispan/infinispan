/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.access;

import org.hibernate.cache.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.factories.ComponentRegistry;
import org.hibernate.cache.spi.access.SoftLock;
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.interceptors.AsyncInterceptorChain;

/**
 * Delegate for non-transactional caches
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class NonTxInvalidationCacheAccessDelegate extends InvalidationCacheAccessDelegate {
   private static final SessionAccess SESSION_ACCESS = SessionAccess.findSessionAccess();
   // FORCE_WRITE_LOCK is a here used as a marker for NonTxInvalidationInterceptor
   // that this is not an eviction and should result in BeginInvalidateCommand
   private static final long REMOVE_FLAGS = FlagBitSets.IGNORE_RETURN_VALUES | FlagBitSets.FORCE_WRITE_LOCK;

   private final AsyncInterceptorChain invoker;
	private final CommandsFactory commandsFactory;
	private final InvocationContextFactory contextFactory;
	private final NonTxPutFromLoadInterceptor nonTxPutFromLoadInterceptor;
   private final boolean isLocal;

	public NonTxInvalidationCacheAccessDelegate(InfinispanDataRegion region, PutFromLoadValidator validator) {
		super(region, validator);
		isLocal = !region.getCache().getCacheConfiguration().clustering().cacheMode().isClustered();
		ComponentRegistry cr = region.getCache().getComponentRegistry();
		invoker = cr.getComponent(AsyncInterceptorChain.class);
		commandsFactory = cr.getComponent(CommandsFactory.class);
		contextFactory = cr.getComponent(InvocationContextFactory.class);
		nonTxPutFromLoadInterceptor = cr.getComponent(NonTxPutFromLoadInterceptor.class);
	}

	@Override
	@SuppressWarnings("UnusedParameters")
	public boolean insert(Object session, Object key, Object value, Object version) throws CacheException {
		if ( !region.checkValid() ) {
			return false;
		}
		write(session, key, value);
		return true;
	}

	@Override
	@SuppressWarnings("UnusedParameters")
	public boolean update(Object session, Object key, Object value, Object currentVersion, Object previousVersion)
			throws CacheException {
		// We update whether or not the region is valid. Other nodes
		// may have already restored the region so they need to
		// be informed of the change.
		write(session, key, value);
		return true;
	}

	@Override
	public void remove(Object session, Object key) throws CacheException {
		// We update whether or not the region is valid. Other nodes
		// may have already restored the region so they need to
		// be informed of the change.
		write(session, key, null);
	}

	private void write(Object session, Object key, Object value) {
		// We need to be invalidating even for regular writes; if we were not and the write was followed by eviction
		// (or any other invalidation), naked put that was started after the eviction ended but before this insert/update
		// ended could insert the stale entry into the cache (since the entry was removed by eviction).
		if (isLocal) {
			// Lock owner is not serialized in local mode so we can use anything (only equals and hashCode are needed)
			Object lockOwner = new Object();
			registerLocalInvalidation(session, lockOwner, key);
			if (!putValidator.beginInvalidatingWithPFER(lockOwner, key, value)) {
				throw log.failedInvalidatePendingPut(key, region.getName());
			}
			// Make use of the simple cache mode here
			cache.remove(key);
		} else {
			RemoveCommand command = commandsFactory.buildRemoveCommand(key, null, REMOVE_FLAGS);
			registerClusteredInvalidation(session, command.getKeyLockOwner(), command.getKey());
			if (!putValidator.beginInvalidatingWithPFER(command.getKeyLockOwner(), key, value)) {
				throw log.failedInvalidatePendingPut(key, region.getName());
			}
			InvocationContext ctx = contextFactory.createSingleKeyNonTxInvocationContext();
			ctx.setLockOwner(command.getKeyLockOwner());
			invoker.invoke(ctx, command);
		}
	}

	@Override
	public boolean afterInsert(Object session, Object key, Object value, Object version) {
		// endInvalidatingKeys is called from NonTxInvalidationInterceptor, from the synchronization callback
		return false;
	}

	@Override
	public boolean afterUpdate(Object session, Object key, Object value, Object currentVersion, Object previousVersion, SoftLock lock) {
		// endInvalidatingKeys is called from NonTxInvalidationInterceptor, from the synchronization callback
		return false;
	}

	@Override
	public void removeAll() throws CacheException {
		try {
			if (!putValidator.beginInvalidatingRegion()) {
				log.failedInvalidateRegion(region.getName());
			}
			cache.clear();
		}
		finally {
			putValidator.endInvalidatingRegion();
		}
	}

	protected void registerLocalInvalidation(Object session, Object lockOwner, Object key) {
		SessionAccess.TransactionCoordinatorAccess transactionCoordinator = SESSION_ACCESS.getTransactionCoordinator(session);
		if (transactionCoordinator == null) {
			return;
		}
		if (trace) {
			log.tracef("Registering synchronization on transaction in %s, cache %s: %s", lockOwner, cache.getName(), key);
		}
		transactionCoordinator.registerLocalSynchronization(new LocalInvalidationSynchronization(putValidator, key, lockOwner));
	}

	protected void registerClusteredInvalidation(Object session, Object lockOwner, Object key) {
		SessionAccess.TransactionCoordinatorAccess transactionCoordinator = SESSION_ACCESS.getTransactionCoordinator(session);
		if (transactionCoordinator == null) {
			return;
		}
		if (trace) {
         log.tracef("Registering synchronization on transaction in %s, cache %s: %s", lockOwner, cache.getName(), key);
      }
		transactionCoordinator.registerLocalSynchronization(new InvalidationSynchronization(nonTxPutFromLoadInterceptor, key, lockOwner));
	}
}
