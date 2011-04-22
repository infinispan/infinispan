/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.lucene.testutils;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;

/**
 * Collects common LuceneSettings for all tests; especially define the backwards compatibility.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
public class LuceneSettings {

   public static final Analyzer analyzer = new SimpleAnalyzer();
   
   private static final MergeScheduler mergeScheduler = new SerialMergeScheduler();
   
   public static IndexWriter openWriter(Directory directory, int maxMergeDocs, boolean useSerialMerger) throws CorruptIndexException, LockObtainFailedException, IOException {
      IndexWriter iwriter = new IndexWriter(directory, LuceneSettings.analyzer, false, MaxFieldLength.UNLIMITED);
      if (useSerialMerger) {
         iwriter.setMergeScheduler(mergeScheduler);
      }
      iwriter.setMaxMergeDocs(maxMergeDocs);
      iwriter.setUseCompoundFile(false);
      return iwriter;
   }
   
   public static IndexWriter openWriter(Directory directory, int maxMergeDocs) throws CorruptIndexException, LockObtainFailedException, IOException {
      return openWriter(directory, maxMergeDocs, false);
   }

}
