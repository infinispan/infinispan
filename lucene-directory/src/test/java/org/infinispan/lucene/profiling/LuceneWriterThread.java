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
