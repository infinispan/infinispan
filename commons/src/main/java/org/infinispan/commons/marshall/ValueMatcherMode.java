package org.infinispan.commons.marshall;

/**
 * Value matcher mode.
 *
 * @deprecated since 9.2
 */
@Deprecated
public enum ValueMatcherMode {

   MATCH_ALWAYS,
   MATCH_EXPECTED,
   MATCH_EXPECTED_OR_NEW,
   MATCH_NON_NULL,
   MATCH_NEVER;

   private static final ValueMatcherMode[] CACHED_VALUES = values();

   public static ValueMatcherMode valueOf(int ordinal) {
      return CACHED_VALUES[ordinal];
   }

}
