package org.horizon.container;

import org.horizon.container.entries.InternalCacheEntry;

import java.util.Collections;
import java.util.LinkedHashMap;

/**
 * A data container that exposes an iterator that is ordered based on least recently used (visited) entries first.
 *
 * //TODO this is a temporary, crappy and *very* inefficient implementation.  Needs to be properly implemented
 * 
 * @author Manik Surtani
 * @since 4.0
 */
public class LRUDataContainer extends FIFODataContainer {
   public LRUDataContainer() {
      entries = Collections.synchronizedMap(new LinkedHashMap<Object, InternalCacheEntry>(16, .75f, true));
   }
}
