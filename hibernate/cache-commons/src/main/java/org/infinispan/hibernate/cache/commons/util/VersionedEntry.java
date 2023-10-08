/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.infinispan.hibernate.cache.commons.util;

import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import org.hibernate.cache.spi.entry.CacheEntry;
import org.hibernate.cache.spi.entry.StructuredCacheEntry;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@ProtoTypeId(ProtoStreamTypeIds.HIBERNATE_VERSIONED_ENTRY)
public class VersionedEntry implements Function<EntryView.ReadWriteEntryView<Object, Object>, Void>, InjectableComponent {
	private static final Log log = LogFactory.getLog(VersionedEntry.class);

	public static final ExcludeEmptyFilter EXCLUDE_EMPTY_VERSIONED_ENTRY = new ExcludeEmptyFilter();
	private final Object value;
	private final Object version;
	private final long timestamp;
	private transient InfinispanDataRegion region;

	public VersionedEntry(long timestamp) {
		this(null, null, timestamp);
	}

	public VersionedEntry(Object value, Object version, long timestamp) {
		this.value = value;
		this.version = version;
		this.timestamp = timestamp;
	}

	@ProtoFactory
	VersionedEntry(MarshallableObject<?> wrappedValue, MarshallableObject<?> wrappedVersion, long timestamp) {
		this(MarshallableObject.unwrap(wrappedValue), MarshallableObject.unwrap(wrappedVersion), timestamp);
	}

	public Object getValue() {
		return value;
	}

	@ProtoField(number = 1, name = "value")
	MarshallableObject<?> getWrappedValue() {
		return MarshallableObject.create(value);
	}

	public Object getVersion() {
		return version;
	}

	@ProtoField(number = 2, name = "version")
	MarshallableObject<?> getWrappedVersion() {
		return MarshallableObject.create(version);
	}

	@ProtoField(3)
	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("VersionedEntry{");
		sb.append("value=").append(value);
		sb.append(", version=").append(version);
		sb.append(", timestamp=").append(timestamp);
		sb.append('}');
		return sb.toString();
	}

	@Override
	public Void apply(EntryView.ReadWriteEntryView<Object, Object> view) {
		if (log.isTraceEnabled()) {
			log.tracef("Applying %s to %s", this, view.find().orElse(null));
		}
		if (version == null) {
			// eviction or post-commit removal: we'll store it with given timestamp
			view.set(this, region.getExpiringMetaParam());
			return null;
		}

		Object oldValue = view.find().orElse(null);
		Object oldVersion = null;
		long oldTimestamp = Long.MIN_VALUE;
		if (oldValue instanceof VersionedEntry) {
			VersionedEntry oldVersionedEntry = (VersionedEntry) oldValue;
			oldVersion = oldVersionedEntry.version;
			oldTimestamp = oldVersionedEntry.timestamp;
			oldValue = oldVersionedEntry.value;
		} else {
			oldVersion = findVersion(oldValue);
		}

		if (oldVersion == null) {
			assert oldValue == null || oldTimestamp != Long.MIN_VALUE : oldValue;
			if (timestamp <= oldTimestamp) {
				// either putFromLoad or regular update/insert - in either case this update might come
				// when it was evicted/region-invalidated. In both cases, with old timestamp we'll leave
				// the invalid value
				assert oldValue == null;
			} else {
				view.set(value instanceof CacheEntry ? value : this);
			}
			return null;
		} else {
			Comparator<Object> versionComparator = null;
			String subclass = findSubclass(value);
			if (subclass != null) {
				versionComparator = region.getComparator(subclass);
				if (versionComparator == null) {
					log.errorf("Cannot find comparator for %s", subclass);
				}
			}
			if (versionComparator == null) {
				view.set(new VersionedEntry(null, null, timestamp), region.getExpiringMetaParam());
			} else {
				int compareResult = versionComparator.compare(version, oldVersion);
				if (log.isTraceEnabled()) {
					log.tracef("Comparing %s and %s -> %d (using %s)", version, oldVersion, compareResult, versionComparator);
				}
				if (value == null && compareResult >= 0) {
					view.set(this, region.getExpiringMetaParam());
				} else if (compareResult > 0) {
					view.set(value instanceof CacheEntry ? value : this);
				}
			}
		}
		return null;
	}

	private Object findVersion(Object entry) {
		if (entry instanceof CacheEntry) {
			// with UnstructuredCacheEntry
			return ((CacheEntry) entry).getVersion();
		} else if (entry instanceof Map) {
			return ((Map) entry).get(StructuredCacheEntry.VERSION_KEY);
		} else {
			return null;
		}
	}

	private String findSubclass(Object entry) {
		// we won't find subclass for structured collections
		if (entry instanceof CacheEntry) {
			return ((CacheEntry) this.value).getSubclass();
		} else if (entry instanceof Map) {
			Object maybeSubclass = ((Map) entry).get(StructuredCacheEntry.SUBCLASS_KEY);
			return maybeSubclass instanceof String ? (String) maybeSubclass : null;
		} else {
			return null;
		}
	}


	@Override
	public void inject(ComponentRegistry registry) {
		region = registry.getComponent(InfinispanDataRegion.class);
	}

	@ProtoTypeId(ProtoStreamTypeIds.HIBERNATE_VERSIONED_ENTRY_EXCLUDE_EMPTY_FILTER)
	public static class ExcludeEmptyFilter implements Predicate<Map.Entry<Object, Object>> {
      @Override
      public boolean test(Map.Entry<Object, Object> entry) {
         if (entry.getValue() instanceof VersionedEntry) {
            return ((VersionedEntry) entry.getValue()).getValue() != null;
         }
         return true;
      }
   }
}
