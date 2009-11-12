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
package org.infinispan.lucene;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Random;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;

public abstract class CacheTestSupport {

   private static final Log log = LogFactory.getLog(CacheTestSupport.class);

   protected static CacheManager createTestCacheManager() {
      return TestCacheManagerFactory.createClusteredCacheManager( createTestConfiguration() );
   }
   
   public static Configuration createTestConfiguration() {
      Configuration c = new Configuration();
      c.setCacheMode(Configuration.CacheMode.DIST_SYNC);
      c.setSyncReplTimeout(10000);
      c.setLockAcquisitionTimeout(10000);
      c.setUseLockStriping(false);
      c.setSyncCommitPhase(true);
      c.setL1CacheEnabled(true);
      c.setExposeJmxStatistics(false);
      c.setUseEagerLocking(false);
      c.setSyncRollbackPhase(true);
      c.setTransactionManagerLookupClass(JBossStandaloneJTAManagerLookup.class.getName());
      c.setDeadlockDetectionSpinDuration( 10000 );
      return c;
   }

   protected static File createDummyDocToIndex(String fileName, int sz) throws Exception {
      File dummyDocToIndex = new File(fileName);
      if (dummyDocToIndex.exists()) {
         dummyDocToIndex.delete();
      }
      dummyDocToIndex.createNewFile();
      Random r = new Random();
      FileWriter fw = new FileWriter(dummyDocToIndex);
      for (int i = 0; i < sz; i++) {
         fw.write(Integer.toHexString(r.nextInt(16)));
      }
      fw.close();
      dummyDocToIndex.deleteOnExit();
      return dummyDocToIndex;
   }

   protected static void doWriteOperation(Directory d, File document) throws Exception {
      // this is a write
      IndexWriter writer = null;
      try {
         writer = new IndexWriter(d, new StandardAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED);
         writer.setMergeScheduler(new SerialMergeScheduler());
         log.info("IndexWriter was constructed");

         Document doc = new Document();
         doc.add(new Field("path", document.getPath(), Field.Store.YES, Field.Index.NOT_ANALYZED));
         doc.add(new Field("modified", DateTools.timeToString(document.lastModified(), DateTools.Resolution.MINUTE),
                  Field.Store.YES, Field.Index.NOT_ANALYZED));
         doc.add(new Field("contents", new FileReader(document)));
         doc.add(new Field("info", "good", Field.Store.YES, Field.Index.ANALYZED));

         writer.addDocument(doc);
      } catch (LockObtainFailedException lofe) {
         // can happen
      } finally {
         if (writer != null) {
            writer.close();
            log.info("IndexWriter was closed");
         }
      }
   }

   protected static void doReadOperation(Directory d) throws Exception {
      IndexSearcher search = null;
      try {
         // this is a read
         search = new IndexSearcher(d);
         // dummy query that probably won't return anything
         QueryParser qp = new QueryParser("path", new StandardAnalyzer());
         search.search(qp.parse("good"), null, 1);
      } finally {
         if (search != null) {
            search.close();
         }
      }
   }

}
