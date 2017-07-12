/**
 * JDBC CacheStore implementation which maps keys to strings.
   If you can guarantee that your application would only use
 * Strings as keys, then this implementation will perform better than binary or mixed
 * implementations.
 *
 * @public
 */
package org.infinispan.persistence.jdbc.stringbased;
