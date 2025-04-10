package org.infinispan.test.hibernate.cache.commons.util;

import java.io.Serializable;

public class TestingKeyFactory {

	private TestingKeyFactory() {
		//Not to be constructed
	}

	public static Object generateEntityCacheKey(String id) {
		return new TestingEntityCacheKey( id );
	}

	public static Object generateCollectionCacheKey(String id) {
		return new TestingEntityCacheKey( id );
	}

	//For convenience implement both interfaces.
	private static class TestingEntityCacheKey implements Serializable {

		private final String id;

		public TestingEntityCacheKey(String id) {
			this.id = id;
		}

		@Override
		public String toString() {
			return id;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			TestingEntityCacheKey other = (TestingEntityCacheKey) obj;
			if (id == null) {
				if (other.id != null)
					return false;
			} else if (!id.equals(other.id))
				return false;
			return true;
		}

	}

}
