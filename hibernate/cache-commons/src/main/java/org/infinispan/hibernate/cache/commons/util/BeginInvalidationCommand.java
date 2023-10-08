/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.util;

import java.util.Arrays;
import java.util.Objects;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@ProtoTypeId(ProtoStreamTypeIds.HIBERNATE_INVALIDATE_COMMAND_BEGIN)
public class BeginInvalidationCommand extends InvalidateCommand {
	private Object lockOwner;

	public BeginInvalidationCommand(long flagsBitSet, CommandInvocationId commandInvocationId, Object[] keys, Object lockOwner) {
		super(flagsBitSet, commandInvocationId, keys);
		this.lockOwner = lockOwner;
	}
	@ProtoFactory
	BeginInvalidationCommand(long flagsWithoutRemote, int topologyId, CommandInvocationId commandInvocationId,
							MarshallableArray<Object> wrappedKeys, MarshallableObject<Object> wrappedLockOwner) {
		super(flagsWithoutRemote, topologyId, commandInvocationId, wrappedKeys);
		this.lockOwner = MarshallableObject.unwrap(wrappedLockOwner);
	}

	@ProtoField(number = 5, name = "lock_owner")
	MarshallableObject<Object> getWrappedLockOwner() {
		return MarshallableObject.create(lockOwner);
	}

	public Object getLockOwner() {
		return lockOwner;
	}

	@Override
	public byte getCommandId() {
		return CacheCommandIds.BEGIN_INVALIDATION;
	}

	@Override
	public boolean equals(Object o) {
		if (!super.equals(o)) {
			return false;
		}
		if (o instanceof BeginInvalidationCommand) {
			BeginInvalidationCommand bic = (BeginInvalidationCommand) o;
			return Objects.equals(lockOwner, bic.lockOwner);
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return super.hashCode() + (lockOwner == null ? 0 : lockOwner.hashCode());
	}

	@Override
	public String toString() {
		return "BeginInvalidationCommand{keys=" + Arrays.toString(keys) +
				", sessionTransactionId=" + lockOwner + '}';
	}
}
