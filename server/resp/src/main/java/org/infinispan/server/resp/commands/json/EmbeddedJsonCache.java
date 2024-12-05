package org.infinispan.server.resp.commands.json;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.server.resp.json.JsonDocBucket;
import org.infinispan.server.resp.json.JsonDocGetFunction;
import org.infinispan.server.resp.json.JsonDocSetFunction;

/**
 * SetCache with Set methods implementation
 *
 * @author Vittorio Rigamonti
 * @since 15.0
 */
public class EmbeddedJsonCache {
   public static final String ERR_KEY_CAN_T_BE_NULL = "key can't be null";
   public static final String ERR_VALUE_CAN_T_BE_NULL = "value can't be null";
   protected final FunctionalMap.ReadWriteMap<byte[], JsonDocBucket> readWriteMap;
   protected final AdvancedCache<byte[], JsonDocBucket> cache;
   protected final InternalEntryFactory entryFactory;

   public EmbeddedJsonCache(Cache<byte[], JsonDocBucket> cache) {
      this.cache = cache.getAdvancedCache();
      FunctionalMapImpl<byte[], JsonDocBucket> functionalMap = FunctionalMapImpl.create(this.cache);
      this.readWriteMap = ReadWriteMapImpl.create(functionalMap);
      this.entryFactory = ComponentRegistry.of(this.cache).getInternalEntryFactory().running();
   }

   /**
    * Get the entry by key and return it as a set
    *
    * @param key, the name of the set
    * @return the set with values if such exist, or null if the key is not present
    */
   public CompletionStage<String> get(byte[] key, String[] paths, String space, String newline, String indent) {
      return readWriteMap.eval(key, new JsonDocGetFunction(paths, space, newline, indent));
   }

   /**
    * Add the specified element to the set, creates the set in case
    *
    * @param key,   the name of the set
    * @param value, the element to be inserted
    * @return {@link CompletionStage} containing a {@link Void}
    */
   public CompletionStage<String> set(byte[] key, String value, String path, boolean nx, boolean xx) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(value, ERR_VALUE_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new JsonDocSetFunction(value, path, nx, xx));
   }
}
