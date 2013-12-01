package org.infinispan.lucene.profiling;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.infinispan.lucene.testutils.LuceneSettings;

/**
 * LuceneWriterThread is going to perform searches on the Directory until it's interrupted.
 * Good for performance comparisons and stress tests.
 * It needs a SharedState object to be shared with other readers and writers on the same directory to
 * be able to throw exceptions in case it's able to detect an illegal state.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
public class LuceneWriterThread extends LuceneUserThread {

   LuceneWriterThread(Directory dir, SharedState state) {
      super(dir, state);
   }

   @Override
   protected void testLoop() throws IOException {
      Set<String> strings = new HashSet<String>();
      int numElements = state.stringsOutOfIndex.drainTo(strings, 5);
      IndexWriter iwriter = LuceneSettings.openWriter(directory, 5000);
      for (String term : strings) {
         Document doc = new Document();
         doc.add(new Field("main", term, Store.NO, Index.NOT_ANALYZED));
         iwriter.addDocument(doc);
      }
      iwriter.commit();
      iwriter.close();
      state.stringsInIndex.addAll(strings);
      state.incrementIndexWriterTaskCount(numElements);
   }

}
