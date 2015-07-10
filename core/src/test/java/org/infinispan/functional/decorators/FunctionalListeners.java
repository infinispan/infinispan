package org.infinispan.functional.decorators;

import org.infinispan.commons.api.functional.Listeners.ReadWriteListeners;
import org.infinispan.commons.api.functional.Listeners.WriteListeners;

public interface FunctionalListeners<K, V> {
   ReadWriteListeners<K, V> readWriteListeners();
   WriteListeners<K, V> writeOnlyListeners();
}
