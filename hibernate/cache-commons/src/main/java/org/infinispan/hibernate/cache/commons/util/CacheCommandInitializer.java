/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.util;

import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.hibernate.cache.commons.access.PutFromLoadValidator;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.LocalModeAddress;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.ByteString;

/**
 * Command initializer
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class CacheCommandInitializer implements ModuleCommandInitializer {

	private final ConcurrentHashMap<String, PutFromLoadValidator> putFromLoadValidators
			= new ConcurrentHashMap<>();

	@Inject Transport transport;

	public void addPutFromLoadValidator(String cacheName, PutFromLoadValidator putFromLoadValidator) {
		// there could be two instances of PutFromLoadValidator bound to the same cache when
		// there are two JndiInfinispanRegionFactories bound to the same cacheManager via JNDI.
		// In that case, as putFromLoadValidator does not really own the pendingPuts cache,
		// it's safe to have more instances.
		putFromLoadValidators.put(cacheName, putFromLoadValidator);
	}

   public PutFromLoadValidator findPutFromLoadValidator(String cacheName) {
      return putFromLoadValidators.get(cacheName);
   }

	public PutFromLoadValidator removePutFromLoadValidator(String cacheName) {
		return putFromLoadValidators.remove(cacheName);
	}

	/**
    * Build an instance of {@link EvictAllCommand} for a given region.
    *
    * @param regionName name of region for {@link EvictAllCommand}
    * @return a new instance of {@link EvictAllCommand}
    */
	public EvictAllCommand buildEvictAllCommand(ByteString regionName) {
		// No need to pass region factory because no information on that object
		// is sent around the cluster. However, when the command factory builds
		// and evict all command remotely, it does need to initialize it with
		// the right region factory so that it can call it back.
		return new EvictAllCommand( regionName );
	}

	public BeginInvalidationCommand buildBeginInvalidationCommand(long flagsBitSet, Object[] keys, Object lockOwner) {
		Address address = transport != null ? transport.getAddress() : LocalModeAddress.INSTANCE;
		return new BeginInvalidationCommand(flagsBitSet, CommandInvocationId.generateId(address), keys, lockOwner);
	}

	public EndInvalidationCommand buildEndInvalidationCommand(ByteString cacheName, Object[] keys, Object lockOwner) {
		return new EndInvalidationCommand( cacheName, keys, lockOwner );
	}

   @Override
	public void initializeReplicableCommand(ReplicableCommand c, boolean isRemote) {
		switch (c.getCommandId()) {
			case CacheCommandIds.END_INVALIDATION:
				EndInvalidationCommand endInvalidationCommand = (EndInvalidationCommand) c;
            endInvalidationCommand.setPutFromLoadValidator(putFromLoadValidators.get(endInvalidationCommand.getCacheName().toString()));
				break;
			case CacheCommandIds.BEGIN_INVALIDATION:
				BeginInvalidationCommand beginInvalidationCommand = (BeginInvalidationCommand) c;
            // FIXME Dan: Module command initializers are global components since 5.1, so we don't have access to the
            // beginInvalidationCommand.init(notifier);
				break;
		}
	}
}
