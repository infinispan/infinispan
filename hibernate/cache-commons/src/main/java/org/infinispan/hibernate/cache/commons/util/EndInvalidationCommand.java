/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.hibernate.cache.commons.access.PutFromLoadValidator;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * Sent in commit phase (after DB commit) to remote nodes in order to stop invalidating
 * putFromLoads.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class EndInvalidationCommand extends BaseRpcCommand {
	private Object[] keys;
	private Object lockOwner;

	public EndInvalidationCommand(ByteString cacheName) {
		this(cacheName, null, null);
	}

	/**
	 * @param cacheName name of the cache to evict
	 */
	public EndInvalidationCommand(ByteString cacheName, Object[] keys, Object lockOwner) {
		super(cacheName);
		this.keys = keys;
		this.lockOwner = lockOwner;
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
	public void writeTo(ObjectOutput output) throws IOException {
		MarshallUtil.marshallArray(keys, output);
      LockOwner.writeTo( output, lockOwner );
	}

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      keys = MarshallUtil.unmarshallArray(input, Util::objectArray);
      lockOwner = LockOwner.readFrom(input);
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
