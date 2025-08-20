package org.infinispan.query.clustered;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.api.query.ClosableIteratorWithCount;
import org.infinispan.commons.api.query.HitCount;
import org.infinispan.commons.query.TotalHitCount;
import org.infinispan.commons.time.TimeService;
import org.infinispan.metadata.Metadata;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.actions.SecurityActions;

/**
 * Iterates on the results of a distributed query returning the values. Subclasses can customize this by overriding the
 * {@link #decorate(Object, Object, float, Metadata)} method.
 *
 * @param <T> The return type of the iterator
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 * @author Sanne Grinovero
 * @since 5.1
 */
class DistributedIterator<T> implements ClosableIteratorWithCount<T> {

   protected final AdvancedCache<Object, Object> cache;

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
   protected final List<T> values;

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
         TopDocs topDocs = partialResults[i].topDocs;
         setShardIndex(topDocs, i);
         partialTopDocs[i] = topDocs;
         i++;
      }
      if (isFieldDocs) {
         mergedResults = TopDocs.merge(sort, firstResult, maxResults, (TopFieldDocs[]) partialTopDocs);
      } else {
         mergedResults = TopDocs.merge(firstResult, maxResults, partialTopDocs);
      }
      batchSize = Math.min(maxResults, cache.getCacheConfiguration().clustering().stateTransfer().chunkSize());
      values = new ArrayList<>(batchSize);
   }

   // Inspired by org.opensearch.action.search.SearchPhaseController
   // from project https://github.com/opensearch-project/
   static void setShardIndex(TopDocs topDocs, int shardIndex) {
      assert topDocs.scoreDocs.length == 0 || topDocs.scoreDocs[0].shardIndex == -1 : "shardIndex is already set";
      for (ScoreDoc doc : topDocs.scoreDocs) {
         doc.shardIndex = shardIndex;
      }
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
   protected T decorate(Object key, Object value, float score, Metadata metadata) {
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
      List<KeyAndScore> keys = new ArrayList<>(batchSize);
      values.clear();
      valueIndex = 0;
      for (int i = 0; i < batchSize; ++i) {
         if (!hasMoreKeys()) {
            break;
         }
         KeyAndScore key = nextKey();
         if (key != null) {
            keys.add(key);
         }
      }

      if (keys.isEmpty()) {
         return;
      }

      if (!queryStatistics.isEnabled()) {
         getAllAndStore(keys);
         return;
      }

      TimeService timeService = SecurityActions.getCacheComponentRegistry(cache).getTimeService();
      long start = timeService.time();
      getAllAndStore(keys);
      queryStatistics.entityLoaded(timeService.timeDuration(start, TimeUnit.NANOSECONDS));
   }

   protected void getAllAndStore(List<KeyAndScore> keysAndScores) {
      var map = cache.getAll(toKeySet(keysAndScores));
      keysAndScores.stream()
            .map(keyAndScore -> decorate(keyAndScore.key(), map.get(keyAndScore.key()), keyAndScore.score(), null))
            .forEach(values::add);
   }

   protected static Set<Object> toKeySet(List<KeyAndScore> keysAndScores) {
      return keysAndScores.stream().map(KeyAndScore::key).collect(Collectors.toSet());
   }

   private Object keyFromStorage(Object key) {
      return cache.getKeyDataConversion().fromStorage(key);
   }

   private boolean hasMoreKeys() {
      int nextIndex = currentIndex + 1;
      return firstResult + nextIndex < resultSize && nextIndex < maxResults;
   }

   private KeyAndScore nextKey() {
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
      return new KeyAndScore(keyFromStorage(nodeTopDocs.keys[pos]), scoreDoc.score);
   }

   @Override
   public HitCount count() {
      return new TotalHitCount(resultSize, true);
   }

   record KeyAndScore(Object key, float score) {
   }
}
