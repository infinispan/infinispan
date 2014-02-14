package org.infinispan.lucene.profiling;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

/**
 * LuceneReaderThread is going to perform searches on the Directory until it's interrupted.
 * Good for performance comparisons and stress tests.
 * It needs a SharedState object to be shared with other readers and writers on the same directory to
 * be able to throw exceptions in case it's able to detect an illegal state.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
public class LuceneReaderThread extends LuceneUserThread {

   protected IndexSearcher searcher;
   protected DirectoryReader indexReader;

   LuceneReaderThread(Directory dir, SharedState state) {
      super(dir, state);
   }

   @Override
   protected void testLoop() throws IOException {
      // take ownership of some strings, so that no other thread will change status for these:
      Set<String> strings = new HashSet<String>();
      int numElements = state.stringsInIndex.drainTo(strings, 50);
      refreshIndexReader();
      for (String term : strings) {
         Query query = new TermQuery(new Term("main", term));
         TopDocs docs = searcher.search(query, null, 1);
         if (docs.totalHits != 1) {
            throw new RuntimeException("String '" + term + "' should exist but was not found in index");
         }
      }
      // put the strings back at their place:
      state.stringsInIndex.addAll(strings);
      state.incrementIndexSearchesCount(numElements);
   }

   protected void refreshIndexReader() throws IOException {
      if (indexReader == null) {
         indexReader = DirectoryReader.open(directory);
      }
      else {
         DirectoryReader before = indexReader;
         DirectoryReader after = DirectoryReader.openIfChanged(indexReader);
         if (after != null) {
            before.close();
            indexReader = after;
         }
      }
      searcher = new IndexSearcher(indexReader);
   }

   @Override
   protected void cleanup() throws IOException {
      if (indexReader != null)
         indexReader.close();
   }

}
