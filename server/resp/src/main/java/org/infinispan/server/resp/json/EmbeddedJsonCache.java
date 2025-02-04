package org.infinispan.server.resp.json;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;

/**
 * JsonCache with Json methods implementation
 *
 * @author Vittorio Rigamonti
 * @since 15.2
 */
public class EmbeddedJsonCache {
   public static final String ERR_KEY_CAN_T_BE_NULL = "key can't be null";
   public static final String ERR_VALUE_CAN_T_BE_NULL = "value can't be null";
   protected final FunctionalMap.ReadWriteMap<byte[], JsonBucket> readWriteMap;
   protected final AdvancedCache<byte[], JsonBucket> cache;
   protected final InternalEntryFactory entryFactory;

   public EmbeddedJsonCache(Cache<byte[], JsonBucket> cache) {
      this.cache = cache.getAdvancedCache();
      FunctionalMapImpl<byte[], JsonBucket> functionalMap = FunctionalMapImpl.create(this.cache);
      this.readWriteMap = ReadWriteMapImpl.create(functionalMap);
      this.entryFactory = ComponentRegistry.of(this.cache).getInternalEntryFactory().running();
   }

   /**
    * Get the entry by key and return it as json byte array
    *
    * @param key, the name of the set
    * @return the set with values if such exist, or null if the key is not present
    */
   public CompletionStage<byte[]> get(byte[] key, List<byte[]> paths, byte[] space, byte[] newline, byte[] indent) {
      return readWriteMap.eval(key, new JsonGetFunction(paths, space, newline, indent));
   }

   /**
    * Add the specified element to the set, creates the set in case
    *
    * @param key,   the name of the set
    * @param value, the element to be inserted
    * @return {@link CompletionStage} containing a {@link Void}
    */
   public CompletionStage<String> set(byte[] key, byte[] value, byte[] path, boolean nx, boolean xx) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(value, ERR_VALUE_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new JsonSetFunction(value, path, nx, xx));
   }

   public CompletionStage<List<Long>> objlen(byte[] key, byte[] path) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new JsonObjlenFunction(path));
   }
}
