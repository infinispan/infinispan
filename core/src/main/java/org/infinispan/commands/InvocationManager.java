package org.infinispan.commands;

import java.util.Collection;

import org.infinispan.util.TimeService;

/**
 * There are three sources of invocation invalidations:
 * 1) primary/backup writer checks expiration
 * 2) expiration thread checks expiration
 * 3) originator-triggered invalidation (ForgetInvocationsCommand)
 *
 * There can't be any concurrent 1) expirations, and 2) should occur in single thread, too
 */
public interface InvocationManager {
   /**
    * @return Time (in milliseconds) after which an {@link org.infinispan.commands.InvocationRecord} should expire.
    */
   long invocationTimeout();

   /**
    * @return Delegated value from {@link TimeService#wallClockTime()}
    */
   long wallClockTime();

   /**
    * @return True if the cache is sync.
    */
   boolean isSynchronous();

   /**
    * Called when given command will not issue any further retries. We will eventually notify current owners of given
    * segments and ask them to remove invocation records with this command id. It is not necessary to contact all
    * possible owners during the command execution as the nodes that are not owners at this moment should eventually
    * drop that entries. There is a chance that write owner will receive the invalidation before the actual entry
    * (including invalid record), but we can leave such cases upon expiration.
    *
    * This method can trigger a synchronous invocation of invalidation on local node; therefore it is important
    * that originator thread (that calls this method) never stores the invocation itself. More precisely, it should not
    * store the entry *after* notifying about the completion, as the invocation record wouldn't be dropped then.
    *
    * In asynchronous caches, it is not possible to find out when a command has completed, and therefore this method
    * shouldn't be called - we'll need to wait until the invocations expire. In async caches even the originator stores
    * the invocation record, because the entry can be stored (TODO incomplete javadoc)
    *
    * @param commandInvocationId
    * @param key
    * @param segment
    */
   void notifyCompleted(CommandInvocationId commandInvocationId, Object key, int segment);

   /**
    * This is a multi-key version of {@link #notifyCompleted(CommandInvocationId, Object, int)}.
    *
    * @param commandInvocationId
    * @param keysBySegment An array of collections of keys where each collection must contain only keys from segment
    *                      equal to the index in this array.
    */
   void notifyCompleted(CommandInvocationId commandInvocationId, Collection<?>[] keysBySegment);

   /**
    * Make sure that all invocation invalidations have been applied. This method should be used only for test purposes.
    */
   void flush();
}
