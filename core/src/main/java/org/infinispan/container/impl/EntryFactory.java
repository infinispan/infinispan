package org.infinispan.container.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.topology.CacheTopology;
import org.infinispan.commons.util.concurrent.CompletableFutures;

/**
 * A factory for constructing {@link org.infinispan.container.entries.MVCCEntry} instances for use in the {@link org.infinispan.context.InvocationContext}.
 * Implementations of this interface would typically wrap an internal {@link org.infinispan.container.entries.CacheEntry}
 * with an {@link org.infinispan.container.entries.MVCCEntry}.
 *
 * <h3>Expected wrapping patterns</h3>
 *
 * {@link EntryWrappingInterceptor} checks {@link CacheTopology#getReadConsistentHash()} and if this node is an owner
 * of the key, it loads the entry from {@link DataContainer}. On the way back through interceptor stack, the entry is
 * committed from {@link EntryWrappingInterceptor} through {@link ClusteringDependentLogic} which checks
 * {@link CacheTopology#getWriteConsistentHash()}.
 * <p>
 * Entry being wrapped is a prerequisite for the command to run and therefore commit the entry, but it's not up to
 * {@link EntryWrappingInterceptor} to make sure the entry is always wrapped - all the interceptors below can expect
 * is <em>(key is in readCH) => (entry is wrapped)</em>. The entry may be wrapped by EWI or other interceptors later,
 * e.g. (but not limited to) when:
 * <ul>
 * <li>entry is in L1
 * <li>entry is fetched from remote node
 * <li>the cache is transactional and command should be executed on origin (but it does not need previous value
 *     - it is then wrapped as null entry)
 * </ul>
 * It is the distribution interceptor that enforces that (entry is read/written by command) => (entry is wrapped),
 * by fetching the remote value, limiting the set of keys in given command (narrowing it) or not executing the command
 * locally at all.
 * <p>
 * If the entry should be read locally but it's not found in DC, the entry will be wrapped by
 * {@link EntryWrappingInterceptor} (either as {@link NullCacheEntry} for reads or other appropriate type for writes).
 * Such entry returns <code>false</code> on {@link CacheEntry#skipLookup()} as it's value is unsure (subsequent
 * interceptors can retrieve the new value from the cache store or remote node and call
 * {@link EntryFactory#wrapExternalEntry} to update the context.
 * <p>
 * With repeatable reads, the value that is context must not be overwritten by value out of the transaction
 * (only commands in this transaction can change the context entry. That's why {@link EntryWrappingInterceptor}
 * calls {@link CacheEntry#setSkipLookup} from the return handler for every command.
 * <p>
 * When a command is retried and repeatable reads are not used, the entry is removed from the context completely
 * and wrapped again by {@link EntryWrappingInterceptor}. When repeatable reads are in use,
 * {@link org.infinispan.container.entries.RepeatableReadEntry} entry keeps the value before the command was executed
 * and the context is reset to this value.
 * <p>
 * This summarizes expected behaviour of interceptors loading from persistence layer:
 * <ul>
 * <li>entry == null:             don't load the entry because this node is not a read owner
 * <li>entry.skipLookup == false: attempt to load the entry
 * <li>entry.skipLookup == true:  don't load the entry because it was already published
 * </ul>
 * Distribution interceptor should behave as follows:
 * <ul>
 * <li>entry == null: If this is a write command, check writeCH and if this node is
 *    <ul>
 *       <li>primary owner: that should not happen as command.topologyId is outdated (the topology is checked
 *           before executing the command and {@link org.infinispan.statetransfer.OutdatedTopologyException} is thrown)
 *       <li>backup owner and {@link VisitableCommand#loadType()} is {@link org.infinispan.commands.VisitableCommand.LoadType#OWNER OWNER}:
 *           retrieve the value from remote node
 *       <li>backup owner that does not need previous value: wrap null
 *       <li>non-owner: don't execute the command (or narrow the set of keys in it)
*     </ul>
 *    If this is a read-only command:
 *    <ul>
 *       <li>If this is the origin, fetch the entry from remote node
 *       <li>If this is not the origin, the command must have different topologyId and we retry
 *    </ul>
 * <li>entry != null: don't do any remote retrieval because the value is known
 * </ul>
 * <p>
 * In local mode, the data can be always read and written, so there is no risk that a command won't have the entry
 * wrapped.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface EntryFactory {
   /**
    * Use to synchronize multiple {@link #wrapEntryForReading(InvocationContext, Object, int, boolean, boolean, CompletionStage)}
    * or {@link #wrapEntryForWriting(InvocationContext, Object, int, boolean, boolean, CompletionStage)} calls.
    *
    * <p>The basic pattern is:</p>
    *
    * <pre>{@code
    * CompletableFuture<Void> initialStage = new CompletableFuture<>();
    * CompletionStage<Void> currentStage = initialStage;
    * for (Object key : ...) {
    *    currentStage = entryFactory.wrapEntryForWriting(..., currentStage);
    * }
    * return asyncInvokeNext(ctx, command, expirationCheckDelay(currentStage, initialStage));
    * }</pre>
    *
    * <p>The effect {@code expirationCheckDelay(currentStage, initialStage)} call is equivalent to completing the
    * {@code initialStage} and returning {@code currentStage}, but it optimizes the common case where
    * {@code currentStage} and {@code initialStage} are the same.</p>
    */
   static CompletionStage<Void> expirationCheckDelay(CompletionStage<Void> currentStage, CompletableFuture<Void> initialStage) {
      if (currentStage == initialStage) {
         // No expiration to process asynchronously, don't bother completing the initial stage
         return CompletableFutures.completedNull();
      }

      // Allow the expiration check to modify the invocation context
      initialStage.complete(null);
      return currentStage;
   }

   /**
    * Wraps an entry for reading.  Usually this is just a raw {@link CacheEntry} but certain combinations of isolation
    * levels and the presence of an ongoing JTA transaction may force this to be a proper, wrapped MVCCEntry.  The entry
    * is also typically placed in the invocation context.
    *
    * @param ctx current invocation context
    * @param key key to look up and wrap
    * @param segment segment for the key
    * @param isOwner true if this node is current owner in readCH (or we ignore CH)
    * @param hasLock true if the invoker already has the lock for this key
    * @param previousStage if wrapping can't be performed synchronously, only access the invocation context
    *                      from another thread after this stage is complete
    * @return stage that when complete the value should be in the context
    */
   CompletionStage<Void> wrapEntryForReading(InvocationContext ctx, Object key, int segment, boolean isOwner,
                                             boolean hasLock, CompletionStage<Void> previousStage);

   /**
    * Insert an entry that exists in the data container into the context.
    *
    * Doesn't do anything if the key was already wrapped.
    *
    * <p>
    * The returned stage will always be complete if <b>isOwner</b> is false.
    *
    * @param ctx current invocation context
    * @param key key to look up and wrap
    * @param segment segment for the key
    * @param isOwner true if this node is current owner in readCH (or we ignore CH)
    * @param isRead true if this operation is expected to read the value of the entry
    * @param previousStage if wrapping can't be performed synchronously, only access the invocation context
    *                      from another thread after this stage is complete
    * @since 8.1
    */
   CompletionStage<Void> wrapEntryForWriting(InvocationContext ctx, Object key, int segment, boolean isOwner, boolean isRead, CompletionStage<Void> previousStage);

   /**
    * Insert an entry that exists in the data container into the context, even if it is expired
    *
    * Doesn't do anything if the key was already wrapped
    * @param ctx current invocation context
    * @param key key to look up and wrap
    * @param segment segment for the key
    * @param isOwner is the local node a read owner?
    */
   void wrapEntryForWritingSkipExpiration(InvocationContext ctx, Object key, int segment, boolean isOwner);

   /**
    * Insert an external entry (e.g. loaded from a cache loader or from a remote node) into the context.
    *
    * @param ctx current invocation context
    * @param key key to look up and wrap
    * @param externalEntry the value to be inserted into context
    * @param isRead true if this operation is expected to read the value of the entry
    * @param isWrite if this is executed within a write command
    * @since 8.1
    */
   void wrapExternalEntry(InvocationContext ctx, Object key, CacheEntry externalEntry, boolean isRead, boolean isWrite);
}
