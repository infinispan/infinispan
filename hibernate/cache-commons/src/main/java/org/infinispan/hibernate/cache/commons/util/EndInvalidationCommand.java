/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.util;

import java.util.Arrays;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.hibernate.cache.commons.access.PutFromLoadValidator;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;

/**
 * Sent in commit phase (after DB commit) to remote nodes in order to stop invalidating
 * putFromLoads.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@ProtoTypeId(ProtoStreamTypeIds.HIBERNATE_INVALIDATE_COMMAND_END)
public class EndInvalidationCommand extends BaseRpcCommand {
	private Object[] keys;
	private Object lockOwner;

	/**
	 * @param cacheName name of the cache to evict
	 */
	public EndInvalidationCommand(ByteString cacheName, Object[] keys, Object lockOwner) {
		super(cacheName);
		this.keys = keys;
		this.lockOwner = lockOwner;
	}

	@ProtoFactory
	EndInvalidationCommand(ByteString cacheName, MarshallableArray<Object> keys, MarshallableObject<Object> lockOwner) {
		this(cacheName, MarshallableArray.unwrap(keys), MarshallableObject.unwrap(lockOwner));
	}

	@ProtoField(2)
	MarshallableArray<Object> getKeys() {
		return MarshallableArray.create(keys);
	}

	@ProtoField(3)
	MarshallableObject<Object> getLockOwner() {
		return MarshallableObject.create(lockOwner);
	}

	@Override
	public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) {
		for (Object key : keys) {
			PutFromLoadValidator putFromLoadValidator = componentRegistry.getComponent(PutFromLoadValidator.class);
			putFromLoadValidator.endInvalidatingKey(lockOwner, key);
		}
		return CompletableFutures.completedNull();
	}

	@Override
	public byte getCommandId() {
		return CacheCommandIds.END_INVALIDATION;
	}

	@Override
	public boolean isReturnValueExpected() {
		return false;
	}

	@Override
	public boolean canBlock() {
		return true;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof EndInvalidationCommand)) {
			return false;
		}

		EndInvalidationCommand that = (EndInvalidationCommand) o;

		if (cacheName == null ? cacheName != null : !cacheName.equals(that.cacheName)) {
			return false;
		}
		if (!Arrays.equals(keys, that.keys)) {
			return false;
		}
		return !(lockOwner != null ? !lockOwner.equals(that.lockOwner) : that.lockOwner != null);

	}

	@Override
	public int hashCode() {
		int result = cacheName != null ? cacheName.hashCode() : 0;
		result = 31 * result + (keys != null ? Arrays.hashCode(keys) : 0);
		result = 31 * result + (lockOwner != null ? lockOwner.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("EndInvalidationCommand{");
		sb.append("cacheName=").append(cacheName);
		sb.append(", keys=").append(Arrays.toString(keys));
		sb.append(", sessionTransactionId=").append(lockOwner);
		sb.append('}');
		return sb.toString();
	}
}
