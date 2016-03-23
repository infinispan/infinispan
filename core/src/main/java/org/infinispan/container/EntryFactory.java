package org.infinispan.container;

import org.infinispan.atomic.Delta;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
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
   enum Wrap {
      /** Store values in the context without wrapping */
      STORE,
      /** Wrap any value */
      WRAP_ALL,
      /** Only wrap non-null values */
      WRAP_NON_NULL,
   }

   /**
    * Wraps an entry for reading.  Usually this is just a raw {@link CacheEntry} but certain combinations of isolation
    * levels and the presence of an ongoing JTA transaction may force this to be a proper, wrapped MVCCEntry.  The entry
    * is also typically placed in the invocation context.
    *
    * @param ctx current invocation context
    * @param key key to look up and wrap
    * @param existing
    * @throws InterruptedException when things go wrong, usually trying to acquire a lock
    */
   CacheEntry wrapEntryForReading(InvocationContext ctx, Object key, CacheEntry existing);

   /**
    * Used for wrapping Delta entry to be applied to DeltaAware object stored in cache. The wrapped
    * entry is added to the supplied InvocationContext.
    */
   CacheEntry wrapEntryForDelta(InvocationContext ctx, Object deltaKey, Delta delta);

   /**
    * Insert an entry that exists in the data container into the context.
    *
    * Doesn't do anything if the key was already wrapped.
    *
    * @return The wrapped entry.
    * @since 8.1
    */
   MVCCEntry wrapEntryForWriting(InvocationContext ctx, Object key, Wrap wrap, boolean skipRead,
                                 boolean ignoreOwnership);

   /**
    * Insert an external entry (e.g. loaded from a cache loader or from a remote node) into the context.
    *
    * Will not replace an existing InternalCacheEntry.
    *
    * @return {@code true} if the context entry was modified, {@code false} otherwise.
    * @since 8.1
    */
   boolean wrapExternalEntry(InvocationContext ctx, Object key, CacheEntry externalEntry, Wrap wrap,
                             boolean skipRead);
   }
