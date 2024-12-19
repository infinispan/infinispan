/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.util;

import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.MetaParam;
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * This is used both as the storage in entry, and for efficiency also directly in the cache.put() commands.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@ProtoTypeId(ProtoStreamTypeIds.HIBERNATE_TOMBSTONE)
public class Tombstone implements Function<EntryView.ReadWriteEntryView<Object, Object>, Void>, InjectableComponent, CompletableFunction {
	public static final ExcludeTombstonesFilter EXCLUDE_TOMBSTONES = new ExcludeTombstonesFilter();

	// the format of data is repeated (timestamp, UUID.LSB, UUID.MSB)
	@ProtoField(1)
	final long[] data;
	private transient InfinispanDataRegion region;
	private transient boolean complete;

	public Tombstone(UUID uuid, long timestamp) {
		this.data = new long[] { timestamp, uuid.getLeastSignificantBits(), uuid.getMostSignificantBits() };
	}

	@ProtoFactory
	Tombstone(long[] data) {
		this.data = data;
	}

	public long getLastTimestamp() {
		long max = data[0];
		for (int i = 3; i < data.length; i += 3) {
			max = Math.max(max, data[i]);
		}
		return max;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("Tombstone{");
		for (int i = 0; i < data.length; i += 3) {
			if (i != 0) {
				sb.append(", ");
			}
			sb.append(new UUID(data[i + 2], data[i + 1])).append('=').append(data[i]);
		}
		sb.append('}');
		return sb.toString();
	}

	public Tombstone merge(Tombstone update) {
		assert update != null;
		assert update.data.length == 3;
		int toRemove = 0;
		for (int i = 0; i < data.length; i += 3) {
			if (data[i] < update.data[0]) {
				toRemove += 3;
			}
			else if (update.data[1] == data[i + 1] && update.data[2] == data[i + 2]) {
				// UUID matches - second update during retry?
				toRemove += 3;
			}
		}
		if (data.length == toRemove) {
			// applying the update second time?
			return update;
		}
		else {
			long[] newData = new long[data.length - toRemove + 3]; // 3 for the update
			int j = 0;
			boolean uuidMatch = false;
			for (int i = 0; i < data.length; i += 3) {
				if (data[i] < update.data[0]) {
					// This is an old eviction
					continue;
				}
				else if (update.data[1] == data[i + 1] && update.data[2] == data[i + 2]) {
					// UUID matches
					System.arraycopy(update.data, 0, newData, j, 3);
					uuidMatch = true;
					j += 3;
				}
				else {
					System.arraycopy(data, i, newData, j, 3);
					j += 3;
				}
			}
			assert (uuidMatch && j == newData.length) || (!uuidMatch && j == newData.length - 3);
			if (!uuidMatch) {
				System.arraycopy(update.data, 0, newData, j, 3);
			}
			return new Tombstone(newData);
		}
	}

	public Object applyUpdate(UUID uuid, long timestamp, Object value) {
		int toRemove = 0;
		for (int i = 0; i < data.length; i += 3) {
			if (data[i] < timestamp) {
				toRemove += 3;
			}
			else if (uuid.getLeastSignificantBits() == data[i + 1] && uuid.getMostSignificantBits() == data[i + 2]) {
				toRemove += 3;
			}
		}
		if (data.length == toRemove) {
			if (value == null) {
				return new Tombstone(uuid, timestamp);
			}
			else {
				return value;
			}
		}
		else {
			long[] newData = new long[data.length - toRemove + 3]; // 3 for the update
			int j = 0;
			boolean uuidMatch = false;
			for (int i = 0; i < data.length; i += 3) {
				if (data[i] < timestamp) {
					// This is an old eviction
					continue;
				}
				else if (uuid.getLeastSignificantBits() == data[i + 1] && uuid.getMostSignificantBits() == data[i + 2]) {
					newData[j] = timestamp;
					newData[j + 1] = uuid.getLeastSignificantBits();
					newData[j + 2] = uuid.getMostSignificantBits();
					uuidMatch = true;
					j += 3;
				}
				else {
					System.arraycopy(data, i, newData, j, 3);
					j += 3;
				}
			}
			assert (uuidMatch && j == newData.length) || (!uuidMatch && j == newData.length - 3);
			if (!uuidMatch) {
				newData[j] = timestamp;
				newData[j + 1] = uuid.getLeastSignificantBits();
				newData[j + 2] = uuid.getMostSignificantBits();
			}
			return new Tombstone(newData);
		}
	}

	// Used only for testing purposes
	public int size() {
		return data.length / 3;
	}

	@Override
	public Void apply(EntryView.ReadWriteEntryView<Object, Object> view) {
		Object storedValue = view.find().orElse(null);
		MetaParam.MetaLifespan expiringMetaParam = region.getExpiringMetaParam();
		if (storedValue instanceof Tombstone) {
			view.set(((Tombstone) storedValue).merge(this), expiringMetaParam);
		} else {
			view.set(this, expiringMetaParam);
		}
		return null;
	}

	@Override
	public void inject(ComponentRegistry registry) {
		region = registry.getComponent(InfinispanDataRegion.class);
	}

	@Override
	public boolean isComplete() {
		return complete;
	}

	@Override
	public void markComplete() {
		complete = true;
	}

	@ProtoTypeId(ProtoStreamTypeIds.HIBERNATE_TOMBSTONE_EXCLUDE_EMPTY_VERSIONED_ENTRY)
	public static class ExcludeTombstonesFilter implements Predicate<Map.Entry<Object, Object>> {

		@ProtoFactory
		ExcludeTombstonesFilter() {}

		@Override
		public boolean test(Map.Entry<Object, Object> entry) {
			return !(entry.getValue() instanceof Tombstone);
		}
	}
}
