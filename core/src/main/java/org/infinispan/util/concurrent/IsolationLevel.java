package org.infinispan.util.concurrent;

/**
 * Various transaction isolation levels as an enumerated class.  Note that Infinispan
 * supports only {@link #READ_COMMITTED} and {@link #REPEATABLE_READ}, upgrading where possible.
 * <p>
 * Also note that Infinispan defaults to {@link #READ_COMMITTED}.
 * <p>
 *
 * @author (various)
 * @see <a href="http://infinispan.org/docs/stable/user_guide/user_guide.html#isolation_levels">Isolation levels</a>
 * @since 4.0
 */
public enum IsolationLevel {
   /**
    * No isolation.
    */
   NONE,
   SERIALIZABLE,
   REPEATABLE_READ,
   READ_COMMITTED,
   READ_UNCOMMITTED
}
