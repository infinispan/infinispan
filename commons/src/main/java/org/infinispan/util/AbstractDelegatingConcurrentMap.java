package org.infinispan.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class AbstractDelegatingConcurrentMap<K, V> extends AbstractDelegatingMap<K, V> implements ConcurrentMap<K, V> {

   protected abstract ConcurrentMap<K, V> delegate();
}
