package org.infinispan.query.clustered;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.encoding.DataConversion;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.remoting.transport.Address;

/**
 * Iterates on the results of a distributed query returning the values. Subclasses can customize this by overriding the
 * {@link #decorate} method.
 *
 * @param <T> The return type of the iterator
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 * @author Sanne Grinovero
 * @since 5.1
 */
class DistributedIterator<T> implements CloseableIterator<T> {

   private final AdvancedCache<?, ?> cache;
   private final DataConversion keyDataConversion;

   private int currentIndex = -1;

   // todo [anistor] seems we are ignoring fetchSize https://issues.jboss.org/browse/ISPN-9506
   private final int fetchSize;
   private final int resultSize;
   private final int maxResults;
   private final int firstResult;
   private final NodeTopDocs[] partialResults;
   private final int[] partialPositionNext;
   private final TopDocs mergedResults;
   private final LocalQueryStatistics queryStatistics;

   DistributedIterator(LocalQueryStatistics queryStatistics, Sort sort, int fetchSize, int resultSize, int maxResults,
                       int firstResult, Map<Address, NodeTopDocs> topDocsResponses, AdvancedCache<?, ?> cache) {
      this.queryStatistics = queryStatistics;
      this.fetchSize = fetchSize;
      this.resultSize = resultSize;
      this.maxResults = maxResults;
      this.firstResult = firstResult;
      this.cache = cache;
      this.keyDataConversion = cache.getKeyDataConversion();
      final int parallels = topDocsResponses.size();
      this.partialResults = new NodeTopDocs[parallels];
      boolean isFieldDocs = expectTopFieldDocs(topDocsResponses);
      TopDocs[] partialTopDocs = isFieldDocs ? new TopFieldDocs[parallels] : new TopDocs[parallels];
      this.partialPositionNext = new int[parallels];
      int i = 0;
      for (Entry<Address, NodeTopDocs> entry : topDocsResponses.entrySet()) {
         partialResults[i] = entry.getValue();
         partialTopDocs[i] = partialResults[i].topDocs;
         i++;
      }
      if (isFieldDocs) {
         mergedResults = TopDocs.merge(sort, firstResult, maxResults, (TopFieldDocs[]) partialTopDocs, true);
      } else {
         mergedResults = TopDocs.merge(firstResult, maxResults, partialTopDocs, true);
      }
   }

   private boolean expectTopFieldDocs(Map<Address, NodeTopDocs> topDocsResponses) {
      Iterator<NodeTopDocs> it = topDocsResponses.values().iterator();
      if (it.hasNext()) {
         return it.next().topDocs instanceof TopFieldDocs;
      }
      return false;
   }

   @Override
   public void close() {
      // Nothing to do
   }

   @Override
   public final T next() {
      if (!hasNext()) {
         throw new NoSuchElementException();
      }

      currentIndex++;

      ScoreDoc scoreDoc = mergedResults.scoreDocs[currentIndex];
      int index = scoreDoc.shardIndex;
      NodeTopDocs nodeTopDocs = partialResults[index];
      if (partialPositionNext[index] == 0) {
         int docId = scoreDoc.doc;
         ScoreDoc[] scoreDocs = nodeTopDocs.topDocs.scoreDocs;
         for (int i = 0; i < scoreDocs.length; i++) {
            if (scoreDocs[i].doc == docId) {
               partialPositionNext[index] = i;
               break;
            }
         }
      }

      int pos = partialPositionNext[index]++;

      Object[] keys = nodeTopDocs.keys;
      if (keys == null || keys.length == 0) {
         return (T) nodeTopDocs.projections[pos];
      }

      long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;

      Object key = keyDataConversion.fromStorage(keys[pos]);
      T value = (T) cache.get(key);

      if (queryStatistics.isEnabled()) queryStatistics.entityLoaded(System.nanoTime() - start);

      return decorate(key, value);
   }

   /**
    * Extension point for subclasses.
    */
   protected T decorate(Object key, Object value) {
      return (T) value;
   }

   @Override
   public final boolean hasNext() {
      int nextIndex = currentIndex + 1;
      return firstResult + nextIndex < resultSize && nextIndex < maxResults;
   }
}
