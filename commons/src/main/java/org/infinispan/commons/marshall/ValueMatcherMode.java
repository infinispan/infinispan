package org.infinispan.commons.marshall;

/**
 * Value matcher mode.
 */
public enum ValueMatcherMode {

   MATCH_ALWAYS,
   MATCH_EXPECTED,
   MATCH_EXPECTED_OR_NEW,
   MATCH_NON_NULL,
   MATCH_NEVER

}
