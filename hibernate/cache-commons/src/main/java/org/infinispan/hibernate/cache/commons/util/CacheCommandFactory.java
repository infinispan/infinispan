/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.util;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.util.ByteString;

/**
 * Command factory
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class CacheCommandFactory implements ModuleCommandFactory {

	@Override
	public Map<Byte, Class<? extends ReplicableCommand>> getModuleCommands() {
		final Map<Byte, Class<? extends ReplicableCommand>> map = new HashMap<Byte, Class<? extends ReplicableCommand>>( 3 );
		map.put( CacheCommandIds.EVICT_ALL, EvictAllCommand.class );
		map.put( CacheCommandIds.END_INVALIDATION, EndInvalidationCommand.class );
		map.put( CacheCommandIds.BEGIN_INVALIDATION, BeginInvalidationCommand.class );
		return map;
	}

	@Override
	public CacheRpcCommand fromStream(byte commandId, ByteString cacheName) {
		return switch (commandId) {
         case CacheCommandIds.EVICT_ALL -> new EvictAllCommand(cacheName);
         case CacheCommandIds.END_INVALIDATION -> new EndInvalidationCommand(cacheName);
         default -> throw new IllegalArgumentException("Not registered to handle command id " + commandId);
      };
	}

	@Override
	public ReplicableCommand fromStream(byte commandId) {
      if (commandId == CacheCommandIds.BEGIN_INVALIDATION) {
         return new BeginInvalidationCommand();
      } else {
         throw new IllegalArgumentException("Not registered to handle command id " + commandId);
      }
	}
}
