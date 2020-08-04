package org.infinispan.query.clustered;

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
import org.infinispan.remoting.transport.Address;

/**
 * Iterates on a distributed query.
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 * @author Sanne Grinovero
 * @since 5.1
 */
class DistributedIterator<E> implements CloseableIterator<E> {

   protected final AdvancedCache<?, ?> cache;
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

   DistributedIterator(Sort sort, int fetchSize, int resultSize, int maxResults, int firstResult, Map<Address, NodeTopDocs> topDocsResponses, AdvancedCache<?, ?> cache) {
      this.fetchSize = fetchSize;
      this.resultSize = resultSize;
      this.maxResults = maxResults;
      this.firstResult = firstResult;
      this.cache = cache;
      this.keyDataConversion = cache.getKeyDataConversion();
      final int parallels = topDocsResponses.size();
      this.partialResults = new NodeTopDocs[parallels];
      TopDocs[] partialTopDocs = sort != null ? new TopFieldDocs[parallels] : new TopDocs[parallels];
      this.partialPositionNext = new int[parallels];
      int i = 0;
      for (Entry<Address, NodeTopDocs> entry : topDocsResponses.entrySet()) {
         partialResults[i] = entry.getValue();
         partialTopDocs[i] = partialResults[i].topDocs;
         i++;
      }
      if (sort != null) {
         mergedResults = TopDocs.merge(sort, firstResult, maxResults, (TopFieldDocs[]) partialTopDocs, true);
      } else {
         mergedResults = TopDocs.merge(firstResult, maxResults, partialTopDocs, true);
      }
   }

   @Override
   public void close() {
      // Nothing to do (extension point)
   }

   @Override
   public E next() {
      if (!hasNext()) {
         throw new NoSuchElementException();
      }

      currentIndex++;
      // fetch and return the value
      ScoreDoc scoreDoc = mergedResults.scoreDocs[currentIndex];
      int index = scoreDoc.shardIndex;
      if (partialPositionNext[index] == 0) {
         partialPositionNext[index] = findSpecificPosition(scoreDoc.doc, partialResults[index].topDocs);
      }
      int specificPosition = partialPositionNext[index];
      partialPositionNext[index]++;
      return fetchValue(specificPosition, partialResults[index]);
   }

   private int findSpecificPosition(int docId, TopDocs topDocs) {
      for (int i = 0; i < topDocs.scoreDocs.length; i++) {
         if (topDocs.scoreDocs[i].doc == docId) return i;
      }
      return 0;
   }

   protected E fetchValue(int scoreIndex, NodeTopDocs nodeTopDocs) {
      Object[] keys = nodeTopDocs.keys;
      if (keys != null && keys.length > 0) {
         return (E) cache.get(keyDataConversion.fromStorage(keys[scoreIndex]));
      }
      return (E) nodeTopDocs.projections[scoreIndex];
   }

   @Override
   public final boolean hasNext() {
      int nextIndex = currentIndex + 1;
      return firstResult + nextIndex < resultSize && nextIndex < maxResults;
   }
}
