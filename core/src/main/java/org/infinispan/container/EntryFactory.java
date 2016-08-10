package org.infinispan.container;

import org.infinispan.atomic.Delta;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;

/**
 * A factory for constructing {@link org.infinispan.container.entries.MVCCEntry} instances for use in the {@link org.infinispan.context.InvocationContext}.
 * Implementations of this interface would typically wrap an internal {@link org.infinispan.container.entries.CacheEntry}
 * with an {@link org.infinispan.container.entries.MVCCEntry}, optionally acquiring the necessary locks via the
 * {@link org.infinispan.util.concurrent.locks.LockManager}.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface EntryFactory {
   /**
    * Wraps an entry for reading.  Usually this is just a raw {@link CacheEntry} but certain combinations of isolation
    * levels and the presence of an ongoing JTA transaction may force this to be a proper, wrapped MVCCEntry.  The entry
    * is also typically placed in the invocation context.
    *
    * @param ctx current invocation context
    * @param key key to look up and wrap
    * @param isOwner true if this node is current owner in readCH (or we ignore CH)
    * @return The entry in context after the call
    */
   void wrapEntryForReading(InvocationContext ctx, Object key, boolean isOwner);

   /**
    * Used for wrapping Delta entry to be applied to DeltaAware object stored in cache. The wrapped
    * entry is added to the supplied InvocationContext.
    *
    * @param ctx current invocation context
    * @param deltaKey key to look up and wrap
    * @param delta the delta of the executed command
    * @param isOwner true if this node is current owner in readCH (or we ignore CH)
    * @return The entry in context after the call
    */
   CacheEntry wrapEntryForDelta(InvocationContext ctx, Object deltaKey, Delta delta, boolean isOwner);

   /**
    * Insert an entry that exists in the data container into the context.
    *
    * Doesn't do anything if the key was already wrapped.
    *
    * @param ctx current invocation context
    * @param key key to look up and wrap
    * @param skipRead true if the version of the entry read should be recorded for WSC
    * @param isOwner true if this node is current owner in readCH (or we ignore CH)
    * @return The entry in context after the call
    * @since 8.1
    */
   MVCCEntry wrapEntryForWriting(InvocationContext ctx, Object key, boolean skipRead, boolean isOwner);

   /**
    * Insert an external entry (e.g. loaded from a cache loader or from a remote node) into the context.
    *
    * Will not replace an existing InternalCacheEntry.
    *
    * @param ctx current invocation context
    * @param key key to look up and wrap
    * @param externalEntry the value to be inserted into context
    * @param isWrite if this is executed within a write command
    * @param skipRead true if the version of the entry read should be recorded for WSC
    * @return {@code true} if the context entry was modified, {@code false} otherwise.
    * @since 8.1
    */
   boolean wrapExternalEntry(InvocationContext ctx, Object key, CacheEntry externalEntry, boolean isWrite, boolean skipRead);
}
