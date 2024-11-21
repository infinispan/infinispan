package org.infinispan.client.hotrod.impl.cache;

import org.infinispan.api.common.CacheEntryVersion;

/**
 * @since 14.0
 **/
public record CacheEntryVersionImpl(long version) implements CacheEntryVersion { }
