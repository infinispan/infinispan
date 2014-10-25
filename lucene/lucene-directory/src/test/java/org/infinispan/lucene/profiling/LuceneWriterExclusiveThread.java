package org.infinispan.lucene.profiling;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.infinispan.lucene.testutils.LuceneSettings;

/**
 * Similar to LuceneWriterThread, except the IndexWriter isn't being
 * closed in each loop iteration but is being reused.
 * This simulates the configuration option exclusive_index_use as applied
 * by Hibernate Search and Infinispan Query.
 *
 * @author Sanne Grinovero
 * @since 7.0
 */
public class LuceneWriterExclusiveThread extends LuceneUserThread {

   private IndexWriter iwriter;

   LuceneWriterExclusiveThread(Directory dir, SharedState state) {
      super(dir, state);
   }

   protected void beforeLoop() throws IOException {
      iwriter = LuceneSettings.openWriter(directory, 5000);
   }

   @Override
   protected void testLoop() throws IOException {
      Set<String> strings = new HashSet<String>();
      int numElements = state.stringsOutOfIndex.drainTo(strings, 5);
      for (String term : strings) {
         Document doc = new Document();
         doc.add(new StringField("main", term, Store.NO));
         iwriter.addDocument(doc);
      }
      iwriter.commit();
      state.stringsInIndex.addAll(strings);
      state.incrementIndexWriterTaskCount(numElements);
   }

   protected void cleanup() throws IOException {
      iwriter.close();
   }

}
