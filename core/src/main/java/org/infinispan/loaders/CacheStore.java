package org.infinispan.loaders;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Set;

/**
 * A specialization of the {@link CacheLoader} interface that can be written to.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheStore extends CacheLoader {

   /**
    * Stores an entry
    *
    * @param entry entry to store
    * @throws CacheLoaderException in the event of problems writing to the store
    */
   void store(InternalCacheEntry entry) throws CacheLoaderException;

   /**
    * Writes contents of the stream to the store.  Implementations should expect that the stream contains data in an
    * implementation-specific format, typically generated using {@link #toStream(java.io.ObjectOutput)}.  While not a
    * requirement, it is recommended that implementations make use of the {@link org.infinispan.marshall.StreamingMarshaller}
    * when dealing with the stream to make use of efficient marshalling.
    * <p/>
    * It is imperative that implementations <b><i>do not</i></b> close the stream after finishing with it.
    * <p/>
    * It is also <b><i>recommended</b></i> that implementations use their own start and end markers on the stream since
    * other processes may write additional data to the stream after the cache store has written to it.  As such, either
    * markers or some other mechanism to prevent the store from reading too much information should be employed when
    * writing to the stream in {@link #fromStream(java.io.ObjectInput)} to prevent data corruption.
    * <p/>
    * It can be assumed that the stream passed in already performs buffering such that the cache store implementation
    * doesn't have to.
    * <p/>
    *
    * @param inputStream stream to read from
    * @throws CacheLoaderException in the event of problems writing to the store
    */
   void fromStream(ObjectInput inputStream) throws CacheLoaderException;

   /**
    * Loads the entire state into a stream, using whichever format is most efficient for the cache loader
    * implementation. Typically read and parsed by {@link #fromStream(java.io.ObjectInput)}.
    * <p/>
    * While not a requirement, it is recommended that implementations make use of the {@link
    * org.infinispan.marshall.StreamingMarshaller} when dealing with the stream to make use of efficient marshalling.
    * <p/>
    * It is imperative that implementations <b><i>do not</i></b> flush or close the stream after finishing with it.
    * <p/>
    * It is also <b><i>recommended</b></i> that implementations use their own start and end markers on the stream since
    * other processes may write additional data to the stream after the cache store has written to it.  As such, either
    * markers or some other mechanism to prevent the store from reading too much information in {@link
    * #fromStream(java.io.ObjectInput)} should be employed, to prevent data corruption.
    * <p/>
    * <p/>
    * It can be assumed that the stream passed in already performs buffering such that the cache store implementation
    * doesn't have to.
    * <p/>
    *
    * @param outputStream stream to write to
    * @throws CacheLoaderException in the event of problems reading from the store
    */
   void toStream(ObjectOutput outputStream) throws CacheLoaderException;

   /**
    * Clears all entries in the store
    *
    * @throws CacheLoaderException in the event of problems writing to the store
    */
   void clear() throws CacheLoaderException;

   /**
    * Removes an entry in the store.
    *
    * @param key key to remove
    * @return true if the entry was removed; false if the entry wasn't found.
    * @throws CacheLoaderException in the event of problems writing to the store
    */
   boolean remove(Object key) throws CacheLoaderException;

   /**
    * Bulk remove operation
    *
    * @param keys to remove
    * @throws CacheLoaderException in the event of problems writing to the store
    */
   void removeAll(Set<Object> keys) throws CacheLoaderException;

   /**
    * Purges expired entries from the store.
    *
    * @throws CacheLoaderException in the event of problems writing to the store
    */
   void purgeExpired() throws CacheLoaderException;

   /**
    * Issues a prepare call with a set of modifications to be applied to the cache store
    *
    * @param modifications modifications to be applied
    * @param tx            transaction identifier
    * @param isOnePhase    if true, there will not be a commit or rollback phase and changes should be flushed
    *                      immediately
    * @throws CacheLoaderException in the event of problems writing to the store
    */
   void prepare(List<? extends Modification> modifications, GlobalTransaction tx, boolean isOnePhase) throws CacheLoaderException;

   /**
    * Commits a transaction that has been previously prepared.
    * <p/>
    * This method <i>may</b> be invoked on a transaction for which there is <i>no</i> prior {@link
    * #prepare(java.util.List, org.infinispan.transaction.xa.GlobalTransaction, boolean)}.  The implementation would
    * need to deal with this case accordingly.  Typically, this would be a no-op, after ensuring any resources attached
    * to the transaction are cleared up.
    * <p/>
    * Also note that this method <i>may</i> be invoked on a thread which is different from the {@link
    * #prepare(java.util.List, org.infinispan.transaction.xa.GlobalTransaction, boolean)} invocation.  As such, {@link
    * ThreadLocal}s should not be relied upon to maintain transaction context.
    * <p/>
    *
    * @param tx tx to commit
    * @throws CacheLoaderException in the event of problems writing to the store
    */
   void commit(GlobalTransaction tx) throws CacheLoaderException;

   /**
    * Rolls back a transaction that has been previously prepared
    * <p/>
    * This method <i>may</b> be invoked on a transaction for which there is <i>no</i> prior {@link
    * #prepare(java.util.List, org.infinispan.transaction.xa.GlobalTransaction, boolean)}.  The implementation would
    * need to deal with this case accordingly.  Typically, this would be a no-op, after ensuring any resources attached
    * to the transaction are cleared up.
    * <p/>
    * Also note that this method <i>may</i> be invoked on a thread which is different from the {@link
    * #prepare(java.util.List, org.infinispan.transaction.xa.GlobalTransaction, boolean)} invocation.  As such, {@link
    * ThreadLocal}s should not be relied upon to maintain transaction context.
    * <p/>
    *
    * @param tx tx to roll back
    */
   void rollback(GlobalTransaction tx);

   /**
    * Returns the configuration object associated to this cache store config.
    */
   CacheStoreConfig getCacheStoreConfig();
}
