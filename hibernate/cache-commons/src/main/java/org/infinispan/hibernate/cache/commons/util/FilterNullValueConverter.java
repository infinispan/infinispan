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
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.Metadata;

public final class FilterNullValueConverter<K, V> extends AbstractKeyValueFilterConverter<K, V, Object> {

	private final KeyValueFilter<K, V> filter;

	public FilterNullValueConverter(KeyValueFilter<K, V> filter) {
		this.filter = filter;
	}

	@Override
	public Object filterAndConvert(K key, V value, Metadata metadata) {
		if ( filter.accept( key, value, metadata ) ) {
			return NullValue.getInstance();
		}

		return null;
	}

	public static final class Externalizer extends AbstractExternalizer<FilterNullValueConverter> {

		@Override
		public Set<Class<? extends FilterNullValueConverter>> getTypeClasses() {
			return Collections.singleton( FilterNullValueConverter.class );
		}

		@Override
		public void writeObject(ObjectOutput output, FilterNullValueConverter object) throws IOException {
			output.writeObject( object.filter );
		}

		@Override
		public FilterNullValueConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
			KeyValueFilter<Object, Object> filter = (KeyValueFilter<Object, Object>) input.readObject();
			return new FilterNullValueConverter( filter );
		}

		@Override
		public Integer getId() {
			return Externalizers.FILTER_NULL_VALUE_CONVERTER;
		}
	}

	public static final class NullValue {

		private static final NullValue INSTANCE = new NullValue();

		private NullValue() {
		}

		public static NullValue getInstance() {
			return NullValue.INSTANCE;
		}

		public static final class Externalizer extends AbstractExternalizer<NullValue> {

			@Override
			public Set<Class<? extends NullValue>> getTypeClasses() {
				return Collections.singleton( NullValue.class );
			}

			@Override
			public void writeObject(ObjectOutput output, NullValue object) {
			}

			@Override
			public NullValue readObject(ObjectInput input) {
				return NullValue.getInstance();
			}

			@Override
			public Integer getId() {
				return Externalizers.NULL_VALUE;
			}
		}
	}

}
