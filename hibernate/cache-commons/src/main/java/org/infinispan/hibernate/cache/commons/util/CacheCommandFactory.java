/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.util.ByteString;

/**
 * Command factory
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class CacheCommandFactory implements ModuleCommandFactory {

   /**
    * Keeps track of regions to which second-level cache specific
    * commands have been plugged.
    */
	private ConcurrentMap<String, InfinispanBaseRegion> allRegions = new ConcurrentHashMap<>();

   /**
    * Add region so that commands can be cleared on shutdown.
    *
    * @param region instance to keep track of
    */
	public void addRegion(InfinispanBaseRegion region) {
		allRegions.put( region.getName(), region );
	}

   /**
    * Clear all regions from this command factory.
    *
    * @param regions collection of regions to clear
    */
	public void clearRegions(Collection<? extends InfinispanBaseRegion> regions) {
		regions.forEach( region -> allRegions.remove( region.getName() ) );
	}

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
		CacheRpcCommand c;
		switch ( commandId ) {
			case CacheCommandIds.EVICT_ALL:
				c = new EvictAllCommand( cacheName, allRegions.get( cacheName.toString() ) );
				break;
			case CacheCommandIds.END_INVALIDATION:
				c = new EndInvalidationCommand(cacheName);
				break;
			default:
				throw new IllegalArgumentException( "Not registered to handle command id " + commandId );
		}
		return c;
	}

	@Override
	public ReplicableCommand fromStream(byte commandId) {
		ReplicableCommand c;
		switch ( commandId ) {
			case CacheCommandIds.BEGIN_INVALIDATION:
				c = new BeginInvalidationCommand();
				break;
			default:
				throw new IllegalArgumentException( "Not registered to handle command id " + commandId );
		}
		return c;
	}

}
