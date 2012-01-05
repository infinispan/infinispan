/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.Configuration.CacheMode;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.lucene.testutils.LuceneSettings;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
@SuppressWarnings("deprecation")
@Test(groups = "profiling", testName = "lucene.profiling.DynamicTopologyStressTest", sequential = true)
public class DynamicTopologyStressTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(DynamicTopologyStressTest.class);

   private static final boolean KEEP_GOING = false;
   private static final boolean VISUAL_PROGRESS_FEEDBACK = false;

   /** Number of seconds the test will run. Should run for some minutes **/
   private static final int SECONDS_TOTAL = 15;

   /** How many Documents to write in each second **/
   private static final int WRITES_PER_SECOND = 100;

   /** Number of Terms written in the index **/
   private static final int INITIAL_INDEX_TERMS = 2000;

   private static final String INDEX_NAME = "unstableIndex";
   private static final String FIELDNAME = "fieldname";

   /** Number of reading nodes **/
   private static final int READERS = 5;

   private final AtomicBoolean failed = new AtomicBoolean(false);
   private volatile int lastWrittenTermId = 0;

   /** Registry of clustered CacheManagers used as readers **/
   private EmbeddedCacheManager[] readers = new EmbeddedCacheManager[READERS];
   private DISCARD[] discardPerNode = new DISCARD[READERS];
   private EmbeddedCacheManager writingNode;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration defaultClusteredConfig = org.infinispan.test.AbstractCacheTest
            .getDefaultClusteredConfig(CacheMode.DIST_SYNC, false);
      defaultClusteredConfig.setInvocationBatchingEnabled(true);
      TransportFlags transportFlags = new TransportFlags().withMerge(true);

      writingNode = addClusterEnabledCacheManager(defaultClusteredConfig, transportFlags);
      for (int i = 0; i < READERS; i++) {
         readers[i] = addClusterEnabledCacheManager(defaultClusteredConfig, transportFlags);
         Cache<Object, Object> cache = readers[i].getCache();
         discardPerNode[i] = TestingUtil.getDiscardForCache(cache);
         TestingUtil.setDelayForCache(cache, 1, 1);
         TestingUtil.blockUntilViewReceived(cache, i + 1, 1000);
      }
   }

   @Test
   void testDirectoryUnstableCluster() throws IOException {
      InfinispanDirectory masterDirectory = new InfinispanDirectory(writingNode.getCache(), INDEX_NAME);
      SharedState sharedIndexState = IndexReadingStressTest.fillDirectory(masterDirectory, INITIAL_INDEX_TERMS);

      ExecutorService executor = Executors.newFixedThreadPool(READERS + 1);
      try {
         executor.execute(new ConstantWritingThread(masterDirectory, sharedIndexState));
         for (int i=0; i<READERS; i++) {
            executor.execute(new ConstantReadingThread(masterDirectory, sharedIndexState));
         }
         executor.shutdown();
         executor.awaitTermination(SECONDS_TOTAL * 2, TimeUnit.SECONDS); //wait for all jobs to finish
      } catch (InterruptedException e) {
         log.error(e);
         assert false : "unexpected interruption";
      }
      assert failed.get() == false;
   }

   public class ConstantWritingThread implements Runnable {

      private final Directory masterDirectory;

      public ConstantWritingThread(Directory masterDirectory,
            SharedState sharedIndexState) {
         this.masterDirectory = masterDirectory;
      }

      @Override
      public void run() {
         while (lastWrittenTermId < (SECONDS_TOTAL * WRITES_PER_SECOND) && failed.get() == false) {
            IndexWriter writer = null;
            try {
               int toWrite = lastWrittenTermId + 1;
               writer = LuceneSettings.openWriter(masterDirectory, 3); // 3 or any low setting
               Document doc = new Document();
               Field field = new Field(FIELDNAME, "HA" + toWrite, Store.YES, Index.NOT_ANALYZED);
               doc.add(field);
               writer.addDocument(doc);
               writer.commit();
               lastWrittenTermId = toWrite;
               if (VISUAL_PROGRESS_FEEDBACK) System.out.println("Written: " + doc);
               Thread.sleep( 1000 / WRITES_PER_SECOND );
            } catch (IOException e) {
               failed(e);
               return;
            } catch (InterruptedException e) {
               failed(e);
               return;
            } finally {
               try {
                  if (writer != null) {
                     writer.close();
                  }
               } catch (IOException e) {
                  failed(e);
                  return;
               }
            }
         }
      }

   }

   private void failed(Exception e) {
      log.error(e);
      if (! KEEP_GOING) {
         failed.set(true);
      }
   }

   public class ConstantReadingThread implements Runnable {

      private final Directory masterDirectory;

      public ConstantReadingThread(Directory masterDirectory, SharedState sharedIndexState) {
         this.masterDirectory = masterDirectory;
      }

      @Override
      public void run() {
         while (lastWrittenTermId < (SECONDS_TOTAL * WRITES_PER_SECOND) && failed.get() == false) {
            IndexReader indexReader = null;
            try {
               int i = lastWrittenTermId;
               indexReader = IndexReader.open(masterDirectory, true);
               IndexSearcher indexSearcher = new IndexSearcher(indexReader);
               if (i==0) continue; // nothing written yet
               String termValue = "HA" + i;
               Query query = new TermQuery(new Term( FIELDNAME, termValue));
               TopDocs docs = indexSearcher.search(query, null, 1);
               if (docs.totalHits != 1) {
                  failed.set(true);
                  log.error("String '" + termValue + "' should exist but was not found in index");
               }
               if (VISUAL_PROGRESS_FEEDBACK) System.out.print(".");
               Thread.sleep( 1 );
            } catch (IOException e) {
               failed(e);
               return;
            } catch (InterruptedException e) {
               failed(e);
               return;
            } finally {
               try {
                  if (indexReader != null) {
                     indexReader.close();
                  }
               } catch (IOException e) {
                  failed(e);
                  return;
               }
            }
         }
      }

   }

}
