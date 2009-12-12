/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.lucene.profiling;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.infinispan.lucene.testutils.LuceneSettings;

/**
 * LuceneUserThread: does several activities on the index, including searching, adding to index and
 * deleting. It checks for some expected state in index, like known strings the index should contain
 * or should not contain at any time.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
public class LuceneUserThread implements Runnable {

   private final Directory directory;
   private final SharedState state;
   
   LuceneUserThread(Directory dir, SharedState state) {
      this.directory = dir;
      this.state = state;
   }

   @Override
   public void run() {
      try {
         state.waitForStart();
      } catch (InterruptedException e1) {
         state.errorManage(e1);
         return;
      }
      while (!state.needToQuit()) {
         try {
            testLoop();
         } catch (Exception e) {
            state.errorManage(e);
         }
      }
   }

   private void testLoop() throws IOException {
      addSomeStrings();
      verifyStringsExistInIndex();
   }

   private void addSomeStrings() throws IOException {
      Set<String> strings = new HashSet<String>();
      state.stringsOutOfIndex.drainTo(strings, 5);
      IndexWriter iwriter = LuceneSettings.openWriter(directory);
      for (String term : strings) {
         Document doc = new Document();
         doc.add(new Field("main", term, Store.NO, Index.NOT_ANALYZED));
         iwriter.addDocument(doc);
      }
      iwriter.commit();
      iwriter.close();
      state.stringsInIndex.addAll(strings);
      state.incrementIndexWriterTaskCount(5);
   }

   private void verifyStringsExistInIndex() throws IOException {
      // take ownership of some strings, so that no other thread will change status for them:
      Set<String> strings = new HashSet<String>();
      state.stringsInIndex.drainTo(strings, 50);
      IndexSearcher searcher = new IndexSearcher(directory, true);
      for (String term : strings) {
         Query query = new TermQuery(new Term("main", term));
         TopDocs docs = searcher.search(query, null, 1);
         if (docs.totalHits != 1) {
            throw new RuntimeException("String '" + term + "' should exist but was not found in index");
         }
      }
      // put the strings back at their place:
      state.stringsInIndex.addAll(strings);
      state.incrementIndexSearchesCount(50);
   }

}
