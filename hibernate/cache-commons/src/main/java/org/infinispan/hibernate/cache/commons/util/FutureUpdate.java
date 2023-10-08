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
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Request to update the tombstone, coming from insert/update/remove operation.
 *
 * This object should *not* be stored in cache.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@ProtoTypeId(ProtoStreamTypeIds.HIBERNATE_FUTURE_UPDATE)
public class FutureUpdate implements Function<EntryView.ReadWriteEntryView<Object, Object>, Void>, InjectableComponent {
	private final UUID uuid;
	private final long timestamp;
	private final Object value;
	private transient InfinispanDataRegion region;

	public FutureUpdate(UUID uuid, long timestamp, Object value) {
		this.uuid = uuid;
		this.timestamp = timestamp;
		this.value = value;
	}

	@ProtoFactory
	FutureUpdate(UUID uuid, long timestamp, MarshallableObject<?> value) {
		this(uuid, timestamp, MarshallableObject.unwrap(value));
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("FutureUpdate{");
		sb.append("uuid=").append(uuid);
		sb.append(", timestamp=").append(timestamp);
		sb.append(", value=").append(value);
		sb.append('}');
		return sb.toString();
	}

	@ProtoField(1)
	public UUID getUuid() {
		return uuid;
	}

	@ProtoField(2)
	public long getTimestamp() {
		return timestamp;
	}

	@ProtoField(3)
	public MarshallableObject<?> getValue() {
		return MarshallableObject.create(value);
	}

	@Override
	public Void apply(EntryView.ReadWriteEntryView<Object, Object> view) {
		Object storedValue = view.find().orElse(null);
		if (storedValue instanceof Tombstone) {
			// Note that the update has to keep tombstone even if the transaction was unsuccessful;
			// before write we have removed the value and we have to protect the entry against stale putFromLoads
			Tombstone tombstone = (Tombstone) storedValue;
			Object result = tombstone.applyUpdate(uuid, timestamp, this.value);
			if (result instanceof Tombstone) {
				view.set(result, region.getExpiringMetaParam());
			} else {
				view.set(result);
			}
		}
		// Else: this is an async future update, and it's timestamp may be vastly outdated
		// We need to first execute the async update and then local one, because if we're on the primary
		// owner the local future update would fail the async one.
		// TODO: There is some discrepancy with TombstoneUpdate handling which does not fail the update
		return null;
	}

	@Override
	public void inject(ComponentRegistry registry) {
		region = registry.getComponent(InfinispanDataRegion.class);
	}
}
