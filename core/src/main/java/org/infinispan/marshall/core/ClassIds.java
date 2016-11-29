package org.infinispan.marshall.core;

/**
 * Identifiers for marshalling class name itself; this comes handy when marshalling arrays
 * and the array component type is interface (and therefore it does not have its AdvancedExternalizer).
 *
 * This identifiers don't clash with {@link org.infinispan.commons.marshall.Ids} and therefore it can use the same range.
 */
interface ClassIds {
   /* 0-15 Java classes */
   int OBJECT = 0;
   int STRING = 1;
   int LIST = 2;
   int MAP_ENTRY = 3;

   /* 0-254 Infinispan internal classes */
   int INTERNAL_CACHE_VALUE = 16;

   /* Do not use this id. External identifier (full integer) follows */
   int MAX_ID = 255;
}
