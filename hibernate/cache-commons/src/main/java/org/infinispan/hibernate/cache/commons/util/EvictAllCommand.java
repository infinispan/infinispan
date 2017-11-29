/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.util;

import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.hibernate.cache.commons.impl.BaseRegion;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.ByteString;

/**
 * Evict all command
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class EvictAllCommand extends BaseRpcCommand {

	private final BaseRegion region;

   /**
    * Evict all command constructor.
    *
    * @param regionName name of the region to evict
    * @param region to evict
    */
	public EvictAllCommand(ByteString regionName, BaseRegion region) {
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
	public Object perform(InvocationContext ctx) throws Throwable {
		// When a node is joining the cluster, it may receive an EvictAllCommand before the regions
		// are started up. It's safe to ignore such invalidation at this point since no data got in.
		if (region != null) {
			region.invalidateRegion();
		}
		return null;
	}

	@Override
	public byte getCommandId() {
		return CacheCommandIds.EVICT_ALL;
	}

	@Override
   public void writeTo(ObjectOutput output) {
		// No-op
	}

	@Override
	public boolean isReturnValueExpected() {
		return false;
	}

	@Override
   public void readFrom(ObjectInput input) {
      // No-op
   }

}
