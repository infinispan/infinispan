package org.infinispan.util.concurrent;

/**
 * @see <a href="http://infinispan.org/docs/stable/user_guide/user_guide.html#isolation_levels">Isolation levels</a>
 * @since 4.0
 * @deprecated use {@link org.infinispan.configuration.cache.IsolationLevel} instead
 */
@Deprecated(forRemoval = true, since = "15.0")
public enum IsolationLevel {
   NONE,
   SERIALIZABLE,
   REPEATABLE_READ,
   READ_COMMITTED,
   READ_UNCOMMITTED;

   public org.infinispan.configuration.cache.IsolationLevel to() {
      return org.infinispan.configuration.cache.IsolationLevel.valueOf(name());
   }

   public static IsolationLevel from(org.infinispan.configuration.cache.IsolationLevel level) {
      return IsolationLevel.valueOf(level.name());
   }
}
