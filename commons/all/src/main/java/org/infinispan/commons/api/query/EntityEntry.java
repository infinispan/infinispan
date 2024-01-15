package org.infinispan.commons.api.query;

public record EntityEntry<K, E>(K key, E value, float score) {

}
