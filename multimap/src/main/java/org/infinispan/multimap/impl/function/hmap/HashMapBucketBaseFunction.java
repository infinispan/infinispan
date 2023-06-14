package org.infinispan.multimap.impl.function.hmap;

import org.infinispan.functional.EntryView;
import org.infinispan.multimap.impl.HashMapBucket;
import org.infinispan.util.function.SerializableFunction;

public abstract class HashMapBucketBaseFunction<K, HK, HV, R>
      implements SerializableFunction<EntryView.ReadWriteEntryView<K, HashMapBucket<HK, HV>>, R> { }
