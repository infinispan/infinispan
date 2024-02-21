package org.infinispan.query.clustered;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.remoting.transport.Address;

/**
 * Iterates on the results of a distributed query returning the values. Subclasses can customize this by overriding the
 * {@link #decorate(Object, Object)} method.
 *
 * @param <T> The return type of the iterator
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 * @author Sanne Grinovero
 * @since 5.1
 */
class DistributedIterator<T> implements CloseableIterator<T> {

   private final AdvancedCache<Object, Object> cache;

   private int currentIndex = -1;

   private final int resultSize;
   private final int maxResults;
   private final int firstResult;
   private final NodeTopDocs[] partialResults;
   private final int[] partialPositionNext;
   private final TopDocs mergedResults;
   private final LocalQueryStatistics queryStatistics;

   private int valueIndex;
   private final int batchSize;
   private final List<T> values;

   DistributedIterator(LocalQueryStatistics queryStatistics, Sort sort, int resultSize, int maxResults,
                       int firstResult, Map<Address, NodeTopDocs> topDocsResponses, AdvancedCache<?, ?> cache) {
      this.queryStatistics = queryStatistics;
      this.resultSize = resultSize;
      this.maxResults = maxResults;
      this.firstResult = firstResult;
      this.cache = (AdvancedCache<Object, Object>) cache;
      int parallels = topDocsResponses.size();
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
      batchSize = Math.min(maxResults, cache.getCacheConfiguration().clustering().stateTransfer().chunkSize());
      values = new ArrayList<>(batchSize);
   }

   private static boolean expectTopFieldDocs(Map<Address, NodeTopDocs> topDocsResponses) {
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

      // hasNext populate the values if returns true
      assert !values.isEmpty();
      assert valueIndex < values.size();

      return values.get(valueIndex++);
   }

   /**
    * Extension point for subclasses.
    */
   protected T decorate(Object key, Object value) {
      return (T) value;
   }

   @Override
   public final boolean hasNext() {
      if (valueIndex == values.size()) {
         fetchBatch();
      }
      return valueIndex < values.size();
   }

   private void fetchBatch() {
      // keep the order
      Set<Object> keys = new LinkedHashSet<>(batchSize);
      values.clear();
      valueIndex = 0;
      for (int i = 0; i < batchSize; ++i) {
         if (!hasMoreKeys()) {
            break;
         }
         Object key = nextKey();
         if (key != null) {
            keys.add(cache.getKeyDataConversion().fromStorage(key));
         }
      }

      if (keys.isEmpty()) {
         return;
      }

      if (!queryStatistics.isEnabled()) {
         getAllAndStore(keys);
         return;
      }

      TimeService timeService = cache.getComponentRegistry().getTimeService();
      long start = timeService.time();
      getAllAndStore(keys);
      queryStatistics.entityLoaded(timeService.timeDuration(start, TimeUnit.NANOSECONDS));
   }

   private void getAllAndStore(Set<Object> keys) {
      Map<Object, Object> map = cache.getAll(keys);
      keys.stream().map(key -> decorate(key, map.get(key))).forEach(values::add);
   }

   private boolean hasMoreKeys() {
      int nextIndex = currentIndex + 1;
      return firstResult + nextIndex < resultSize && nextIndex < maxResults;
   }

   private Object nextKey() {
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

      if (nodeTopDocs.keys == null || nodeTopDocs.keys.length == 0) {
         values.add((T) nodeTopDocs.projections[pos]);
         return null;
      }
      return nodeTopDocs.keys[pos];
   }
}
