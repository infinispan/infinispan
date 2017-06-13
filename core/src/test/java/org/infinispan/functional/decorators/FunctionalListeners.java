package org.infinispan.functional.decorators;

import org.infinispan.functional.Listeners.ReadWriteListeners;
import org.infinispan.functional.Listeners.WriteListeners;

public interface FunctionalListeners<K, V> {
   ReadWriteListeners<K, V> readWriteListeners();
   WriteListeners<K, V> writeOnlyListeners();
}
