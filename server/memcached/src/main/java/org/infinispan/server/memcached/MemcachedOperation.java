package org.infinispan.server.memcached;

/**
 * Memcached operations.
 *
 * @author Galder Zamarreño
 * @since 4.1
 * @deprecated since 10.1. Will be removed unless a binary protocol encoder/decoder is implemented.
 */
@Deprecated
public enum MemcachedOperation {
   PutRequest,
   TouchRequest,
   PutIfAbsentRequest,
   ReplaceRequest,
   ReplaceIfUnmodifiedRequest,
   GetRequest,
   GetWithVersionRequest,
   RemoveRequest,
   StatsRequest,
   AppendRequest,
   PrependRequest,
   IncrementRequest,
   DecrementRequest,
   FlushAllRequest,
   VersionRequest,
   VerbosityRequest,
   QuitRequest
   ;
}
