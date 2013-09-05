package org.infinispan.commons.marshall;

/**
 * Indexes for object types included in commons.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
public interface Ids {
   int MURMURHASH_2 = 71;
   int MURMURHASH_2_COMPAT = 72;
   int MURMURHASH_3 = 73;

   int EMPTY_SET = 88;
   int EMPTY_MAP = 89;
   int EMPTY_LIST = 90;
   // internal collections (id=18 no longer in use, might get reused at a later stage)
   int IMMUTABLE_MAP = 19;
   int BYTE_BUFFER = 106;
}
