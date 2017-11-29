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

import org.infinispan.commands.CommandInvocationId;

final class LockOwner {

	private LockOwner() {
	}

	static void writeTo(ObjectOutput out, Object lockOwner) throws IOException {
		if (lockOwner instanceof CommandInvocationId) {
			out.writeByte( 0 );
			CommandInvocationId.writeTo( out, (CommandInvocationId) lockOwner );
		}
		else {
			out.writeByte( 1 );
			out.writeObject(lockOwner);
		}
	}

	static Object readFrom(ObjectInput in) throws IOException, ClassNotFoundException {
		byte lockOwnerType = in.readByte();
		switch ( lockOwnerType ) {
			case 0:
				return CommandInvocationId.readFrom( in );
			case 1:
				return in.readObject();
			default:
				throw new IllegalStateException( "Unknown lock owner type" + lockOwnerType );
		}
	}

}
