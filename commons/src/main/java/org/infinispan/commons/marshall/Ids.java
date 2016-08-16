package org.infinispan.commons.marshall;

/**
 * Indexes for object types included in commons.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
public interface Ids {
   // 71 used to be MURMURHASH_2
   // 72 used to be MURMURHASH_2_COMPAT
   int MURMURHASH_3 = 73;

   int EMPTY_SET = 88;
   int EMPTY_MAP = 89;
   int EMPTY_LIST = 90;

   int IMMUTABLE_LIST = 18;
   int IMMUTABLE_MAP = 19;
   int BYTE_BUFFER = 106;

   // Functional lambdas
   int LAMBDA_CONSTANT = 158;
   int LAMBDA_SET_VALUE_IF_EQUALS_RETURN_BOOLEAN = 159;
   int LAMBDA_WITH_METAS = 160;

   int IMMUTABLE_ENTRY = 167;

   int IMMUTABLE_SET = 173;
}
