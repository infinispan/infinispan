package org.infinispan.hibernate.cache.commons.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import org.infinispan.commons.marshall.AdvancedExternalizer;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class Externalizers {

	public final static int UUID = 1200;
	public final static int TOMBSTONE = 1201;
	public final static int EXCLUDE_TOMBSTONES_FILTER = 1202;
	public final static int TOMBSTONE_UPDATE = 1203;
	public final static int FUTURE_UPDATE = 1204;
	public final static int VALUE_EXTRACTOR = 1205;
	public final static int VERSIONED_ENTRY = 1206;
	public final static int EXCLUDE_EMPTY_VERSIONED_ENTRY = 1207;
   public final static int FILTER_NULL_VALUE_CONVERTER = 1208;
   public final static int NULL_VALUE = 1209;

	public static class UUIDExternalizer implements AdvancedExternalizer<UUID> {

		@Override
		public Set<Class<? extends UUID>> getTypeClasses() {
			return Collections.<Class<? extends UUID>>singleton(UUID.class);
		}

		@Override
		public Integer getId() {
			return UUID;
		}

		@Override
		public void writeObject(ObjectOutput output, UUID uuid) throws IOException {
			output.writeLong(uuid.getMostSignificantBits());
			output.writeLong(uuid.getLeastSignificantBits());
		}

		@Override
		public UUID readObject(ObjectInput input) throws IOException, ClassNotFoundException {
			return new UUID(input.readLong(), input.readLong());
		}
	}
}
