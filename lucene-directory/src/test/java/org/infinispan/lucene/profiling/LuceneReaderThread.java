/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
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
   protected IndexReader indexReader;

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

   protected void refreshIndexReader() throws CorruptIndexException, IOException {
      if (indexReader == null) {
         indexReader = IndexReader.open(directory, true);
      }
      else {
         IndexReader before = indexReader;
         indexReader = indexReader.reopen();
         if (before != indexReader) {
            before.close();
         }
      }
      if (searcher != null) {
         searcher.close();
      }
      searcher = new IndexSearcher(indexReader);
   }
   
   @Override
   protected void cleanup() throws IOException {
      if (indexReader != null)
         indexReader.close();
   }

}
