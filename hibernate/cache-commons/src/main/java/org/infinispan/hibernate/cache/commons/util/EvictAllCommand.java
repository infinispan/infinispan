/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.util;

import java.util.concurrent.CompletionStage;

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

   /**
    * Evict all command constructor.
    *
    * @param cacheName name of the region to evict
    */
   @ProtoFactory
   public EvictAllCommand(ByteString cacheName) {
		super(cacheName);
	}

	@Override
	public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
		// When a node is joining the cluster, it may receive an EvictAllCommand before the regions
		// are started up. It's safe to ignore such invalidation at this point since no data got in.
		var region = registry.getComponent(InfinispanBaseRegion.class);
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
