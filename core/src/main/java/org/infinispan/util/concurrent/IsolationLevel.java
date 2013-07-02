package org.infinispan.util.concurrent;

/**
 * Various transaction isolation levels as an enumerated class.  Note that <a href="http://wiki.jboss.org/wiki/JBossCacheMVCC">MVCC</a>
 * only supports {@link #READ_COMMITTED} and {@link #REPEATABLE_READ}, upgrading where possible.
 * <p/>
 * Also note that Infinispan defaults to {@link #READ_COMMITTED}.
 * <p/>
 *
 * @author (various)
 * @see <a href="http://en.wikipedia.org/wiki/Isolation_%28computer_science%29">Isolation levels</a>
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
