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
import java.util.Objects;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.write.InvalidateCommand;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class BeginInvalidationCommand extends InvalidateCommand {
	private Object lockOwner;

	public BeginInvalidationCommand() {
	}

	public BeginInvalidationCommand(long flagsBitSet, CommandInvocationId commandInvocationId, Object[] keys, Object lockOwner) {
		super(flagsBitSet, commandInvocationId, keys);
		this.lockOwner = lockOwner;
	}

	public Object getLockOwner() {
		return lockOwner;
	}

	@Override
	public void writeTo(ObjectOutput output) throws IOException {
		super.writeTo(output);
      LockOwner.writeTo( output, lockOwner );
	}

	@Override
	public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
		super.readFrom(input);
		lockOwner = LockOwner.readFrom( input );
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
