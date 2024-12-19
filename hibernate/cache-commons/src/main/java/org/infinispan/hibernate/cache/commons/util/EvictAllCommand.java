/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.util;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;

/**
 * Evict all command
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.HIBERNATE_EVICT_ALL_COMMAND)
public class EvictAllCommand extends BaseRpcCommand {

	final static ConcurrentMap<ByteString, InfinispanBaseRegion> allRegions = new ConcurrentHashMap<>();

	/**
	 * Add region so that commands can be cleared on shutdown.
	 *
	 * @param region instance to keep track of
	 */
	public static void addRegion(InfinispanBaseRegion region) {
		allRegions.put(ByteString.fromString(region.getCache().getName()), region);
	}

	/**
	 * Clear all regions from this command factory.
	 *
	 * @param regions collection of regions to clear
	 */
	public static void clearRegions(Collection<? extends InfinispanBaseRegion> regions) {
		regions.forEach(region -> allRegions.remove(ByteString.fromString(region.getCache().getName())));
	}

	private final InfinispanBaseRegion region;

	@ProtoFactory
	static EvictAllCommand protoFactory(ByteString cacheName) {
		return new EvictAllCommand(cacheName, allRegions.get(cacheName));
	}

   /**
    * Evict all command constructor.
    *
    * @param regionName name of the region to evict
    * @param region to evict
    */
	public EvictAllCommand(ByteString regionName, InfinispanBaseRegion region) {
		// region name and cache names are the same...
		super( regionName );
		this.region = region;
	}

   /**
    * Evict all command constructor.
    *
    * @param regionName name of the region to evict
    */
	public EvictAllCommand(ByteString regionName) {
		this( regionName, null );
	}

	@Override
	public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
		// When a node is joining the cluster, it may receive an EvictAllCommand before the regions
		// are started up. It's safe to ignore such invalidation at this point since no data got in.
		if (region != null) {
			region.invalidateRegion();
		}
		return CompletableFutures.completedNull();
	}

	@Override
	public byte getCommandId() {
		return CacheCommandIds.EVICT_ALL;
	}

	@Override
	public boolean isReturnValueExpected() {
		return false;
	}
}
