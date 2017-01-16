package org.infinispan.commands.write;

import org.infinispan.Cache;

/**
 * A policy for determining if a write command should be executed based on the current value in the cache.
 *
 * When retrying conditional write commands in non-transactional caches, it is also used to determine the appropriate
 * return value. E.g. if a {@code putIfAbsent(k, v)} already succeeded on a backup owner which became the primary owner,
 * when retrying the command will find {@code v} in the cache but should return {@code null}. For non-conditional
 * commands it's impossible to know what the previous value was, so the command is allowed to return {@code v}.
 *
 * @author Dan Berindei
 * @since 6.0
 */
public enum ValueMatcher {
   /**
    * Always match. Used when the command is not conditional or when the value was already checked
    * (on the primary owner, in non-tx caches, or on the originator, in tx caches).
    * Also used when retrying {@link Cache#remove(Object)} operations.
    */
   MATCH_ALWAYS() {
      @Override
      public boolean matches(Object existingValue, Object expectedValue, Object newValue) {
         return true;
      }

      @Override
      public boolean nonExistentEntryCanMatch() {
         return true;
      }

      @Override
      public ValueMatcher matcherForRetry() {
         return MATCH_ALWAYS;
      }
   },
   /**
    * Match when the existing value is equal to the expected value.
    * Used for {@link Cache#putIfAbsent(Object, Object)}, {@link Cache#replace(Object, Object, Object)},
    * and {@link Cache#remove(Object, Object)}.
    */
   MATCH_EXPECTED() {
      @Override
      public boolean matches(Object existingValue, Object expectedValue, Object newValue) {
         return existingValue == null ? expectedValue == null : existingValue.equals(expectedValue);
      }

      @Override
      public boolean nonExistentEntryCanMatch() {
         return true;
      }

      @Override
      public ValueMatcher matcherForRetry() {
         return MATCH_EXPECTED_OR_NEW;
      }
   },
   /**
    * Match when the existing value is equal to the expected value or to the new value.
    * Used only in non-tx caches, when retrying a conditional command on the primary owner.
    */
   MATCH_EXPECTED_OR_NEW() {
      @Override
      public boolean matches(Object existingValue, Object expectedValue, Object newValue) {
         if (existingValue == null)
            return expectedValue == null || newValue == null;
         else
            return existingValue.equals(expectedValue) || existingValue.equals(newValue);
      }

      @Override
      public boolean nonExistentEntryCanMatch() {
         return true;
      }

      @Override
      public ValueMatcher matcherForRetry() {
         return MATCH_EXPECTED_OR_NEW;
      }
   },

   MATCH_EXPECTED_OR_NULL() {
      @Override
      public boolean matches(Object existingValue, Object expectedValue, Object newValue) {
         return existingValue == null || existingValue.equals(expectedValue);
      }

      @Override
      public boolean nonExistentEntryCanMatch() {
         return true;
      }

      @Override
      public ValueMatcher matcherForRetry() {
         return MATCH_EXPECTED_OR_NULL;
      }
   },
   /**
    * Match any non-null value. Used for {@link Cache#replace(Object, Object)} and {@link Cache#remove(Object)}.
    */
   MATCH_NON_NULL() {
      @Override
      public boolean matches(Object existingValue, Object expectedValue, Object newValue) {
         return existingValue != null;
      }

      @Override
      public boolean nonExistentEntryCanMatch() {
         return false;
      }

      @Override
      public ValueMatcher matcherForRetry() {
         return MATCH_ALWAYS;
      }
   },
   /**
    * Never match. Only used in transactional mode, as unsuccessful commands are still sent remotely,
    * even though they should not be performed.
    */
   MATCH_NEVER() {
      @Override
      public boolean matches(Object existingValue, Object expectedValue, Object newValue) {
         return false;
      }

      @Override
      public boolean nonExistentEntryCanMatch() {
         return false;
      }

      @Override
      public ValueMatcher matcherForRetry() {
         return MATCH_NEVER;
      }
   },;

   public abstract boolean matches(Object existingValue, Object expectedValue, Object newValue);

   /**
    * @deprecated Since 9.0, no longer used.
    */
   @Deprecated
   public abstract boolean nonExistentEntryCanMatch();

   public abstract ValueMatcher matcherForRetry();

   private static final ValueMatcher[] CACHED_VALUES = values();

   public static ValueMatcher valueOf(int ordinal) {
      return CACHED_VALUES[ordinal];
   }

}
