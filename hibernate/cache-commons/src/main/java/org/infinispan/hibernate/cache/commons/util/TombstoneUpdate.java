/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.util;

import java.util.UUID;
import java.util.function.Function;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.MetaParam;
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Request to update cache either as a result of putFromLoad (if {@link #getValue()} is non-null
 * or evict (if it is null).
 *
 * This object should *not* be stored in cache.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@ProtoTypeId(ProtoStreamTypeIds.HIBERNATE_TOMBSTONE_UPDATE)
public class TombstoneUpdate<T> implements Function<EntryView.ReadWriteEntryView<Object, Object>, Void>, InjectableComponent {
	private static final UUID ZERO = new UUID(0, 0);

	private final long timestamp;
	private final T value;
	private transient InfinispanDataRegion region;

	public TombstoneUpdate(long timestamp, T value) {
		this.timestamp = timestamp;
		this.value = value;
	}

	@ProtoFactory
	TombstoneUpdate(long timestamp, MarshallableObject<T> wrappedValue) {
		this(timestamp, MarshallableObject.unwrap(wrappedValue));
	}

	@ProtoField(1)
	public long getTimestamp() {
		return timestamp;
	}

	public T getValue() {
		return value;
	}

	@ProtoField(number = 2, name = "value")
	MarshallableObject<T> getWrappedValue() {
		return MarshallableObject.create(value);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("TombstoneUpdate{");
		sb.append("timestamp=").append(timestamp);
		sb.append(", value=").append(value);
		sb.append('}');
		return sb.toString();
	}

	@Override
	public Void apply(EntryView.ReadWriteEntryView<Object, Object> view) {
		Object storedValue = view.find().orElse(null);

		if (value == null) {
			if (storedValue != null && !(storedValue instanceof Tombstone)) {
				// We have to keep Tombstone, because otherwise putFromLoad could insert a stale entry
				// (after it has been already updated and *then* evicted)
				view.set(new Tombstone(ZERO, timestamp), region.getExpiringMetaParam());
			}
			// otherwise it's eviction
		} else if (storedValue instanceof Tombstone) {
			Tombstone tombstone = (Tombstone) storedValue;
			if (tombstone.getLastTimestamp() < timestamp) {
				view.set(value);
			}
		} else if (storedValue == null) {
			// async putFromLoads shouldn't cross the invalidation timestamp
			if (region.getLastRegionInvalidation() < timestamp) {
				view.set(value);
			}
		} else {
			// Don't do anything locally. This could be the async remote write, though, when local
			// value has been already updated: let it propagate to remote nodes, too
			view.set(storedValue, view.findMetaParam(MetaParam.MetaLifespan.class).get());
		}
		return null;
	}

	@Override
	public void inject(ComponentRegistry registry) {
		region = registry.getComponent(InfinispanDataRegion.class);
	}
}
