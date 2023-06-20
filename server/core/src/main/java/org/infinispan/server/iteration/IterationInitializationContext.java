package org.infinispan.server.iteration;

import java.util.stream.Stream;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.stream.impl.local.AbstractLocalCacheStream;

public interface IterationInitializationContext {

   AbstractLocalCacheStream.StreamSupplier<CacheEntry<Object, Object>, Stream<CacheEntry<Object, Object>>> getBaseStream();
}
