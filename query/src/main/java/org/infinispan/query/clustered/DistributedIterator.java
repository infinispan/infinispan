package org.infinispan.query.clustered;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.infinispan.AdvancedCache;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.Map.Entry;

/**
 * DistributedIterator.
 *
 * Iterates on a distributed query.
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 * @author Sanne Grinovero
 * @since 5.1
 */
public class DistributedIterator implements ResultIterator {

   private static final Log log = LogFactory.getLog(DistributedIterator.class, Log.class);

   protected final AdvancedCache<?, ?> cache;

   private int currentIndex = -1;

   private final int fetchSize;
   private final int resultSize;
   private final int maxResults;
   private final int firstResult;
   private final UUID[] clusterIDs;
   private final ClusteredTopDocs[] partialResults;
   private final TopDocs[] partialTopDocs;
   private final int[] partialPositionNext;
   private final TopDocs mergedResults;

   public DistributedIterator(Sort sort, int fetchSize, int resultSize, int maxResults, int firstResult, HashMap<UUID, ClusteredTopDocs> topDocsResponses, AdvancedCache<?, ?> cache) {
      this.fetchSize = fetchSize;
      this.resultSize = resultSize;
      this.maxResults = maxResults;
      this.firstResult = firstResult;
      this.cache = cache;
      final int parallels = topDocsResponses.size();
      this.clusterIDs = new UUID[parallels];
      this.partialResults = new ClusteredTopDocs[parallels];
      this.partialTopDocs = new TopDocs[parallels];
      this.partialPositionNext = new int[parallels];
      int i=0;
      for (Entry<UUID, ClusteredTopDocs> entry : topDocsResponses.entrySet()) {
         clusterIDs[i] = entry.getKey();
         partialResults[i] = entry.getValue();
         partialTopDocs[i] = partialResults[i].getTopDocs();
         i++;
      }
      try {
         mergedResults = TopDocs.merge(sort, firstResult, maxResults, partialTopDocs);
      } catch (IOException e) {
         throw log.unexpectedIOException(e);
      }
   }

   @Override
   public void close() {
      // Nothing to do (extension point)
   }

   @Override
   public Object next() {
      if (!hasNext())
         throw new NoSuchElementException("Out of boundaries");
      currentIndex++;
      // fetch and return the value
      ScoreDoc scoreDoc = mergedResults.scoreDocs[currentIndex];
      int index = scoreDoc.shardIndex;
      int specificPosition = partialPositionNext[index];
      partialPositionNext[index]++;
      return fetchValue(specificPosition, partialResults[index]);
   }

   public Object fetchValue(int scoreIndex, ClusteredTopDocs topDoc) {
      ISPNEagerTopDocs eagerTopDocs = (ISPNEagerTopDocs) topDoc.getTopDocs();
      return cache.get(eagerTopDocs.keys[scoreIndex]);
   }

   @Override
   public final void remove() {
      //TODO implement it?
      throw new UnsupportedOperationException("This Iterator is read only");
   }

   @Override
   public final boolean hasNext() {
      int nextIndex = currentIndex + 1;
      if (firstResult + nextIndex >= resultSize || nextIndex >= maxResults) {
         return false;
      }
      return true;
   }

}
