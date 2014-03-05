/**
 * This JDBC CacheStore implementation is optimized for storing String
 * keys in the cache.  If you can guarantee that your application would only use
 * Strings as keys, then this implementation will perform better than binary or mixed
 * implementations.
 *
 * @public
 */
package org.infinispan.persistence.jdbc.stringbased;