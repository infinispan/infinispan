/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.lucene.cacheloader;

import java.io.File;
import java.io.IOException;

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
import org.apache.lucene.store.FSDirectory;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.InfinispanDirectory;
import org.infinispan.lucene.cachestore.LuceneCacheLoader;
import org.infinispan.lucene.cachestore.LuceneCacheLoaderConfig;
import org.infinispan.lucene.testutils.LuceneSettings;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verify we can write to a FSDirectory, and when using it via the {@link LuceneCacheLoader}
 * we can find the same contents as by searching it directly.
 * This implicitly verifies configuration settings passed it, as it won't work if the CacheLoader
 * is unable to find the specific index path.
 *
 * @author Sanne Grinovero
 * @since 5.2
 */
@Test(groups = "functional", testName = "lucene.cacheloader.IndexCacheLoaderTest")
public class IndexCacheLoaderTest {

   private static final String rootDirectoryName = "indexesRootDirTmp";
   private static final int SCALE = 600;

   @Test
   public void readExistingIndexTest() throws IOException {
      File rootDir = new File(new File("."), rootDirectoryName);
      boolean directoriesCreated = rootDir.mkdirs();
      assert directoriesCreated : "couldn't create directory for test";
      try {
         createIndex(rootDir, "index-A", 10 * SCALE, true);
         createIndex(rootDir, "index-B", 20 * SCALE, false);
         verifyIndex(rootDir, "index-A", 10 * SCALE, true);
         verifyIndex(rootDir, "index-B", 20 * SCALE, false);
      }
      finally {
         TestingUtil.recursiveFileRemove(rootDir);
      }
   }

   private void verifyIndex(File rootDir, String indexName, int termsAdded, boolean inverted) throws IOException {
      File indexDir = new File(rootDir, indexName);
      Directory directory = FSDirectory.open(indexDir);
      try {
         verifyOnDirectory(directory, indexName, termsAdded, inverted);
      }
      finally {
         directory.close();
      }
      EmbeddedCacheManager cacheManager = initializeInfinispan(rootDir, indexName);
      try {
         directory = new InfinispanDirectory(cacheManager.getCache(), indexName);
         try {
            verifyOnDirectory(directory, indexName, termsAdded, inverted);
         }
         finally {
            directory.close();
         }
      }
      finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   private EmbeddedCacheManager initializeInfinispan(File rootDir, String indexName) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
         .loaders()
            .addLoader()
               .cacheLoader( new LuceneCacheLoader() )
                  .addProperty(LuceneCacheLoaderConfig.LOCATION_OPTION, rootDir.getAbsolutePath())
                  .addProperty(LuceneCacheLoaderConfig.AUTO_CHUNK_SIZE_OPTION, "1024");
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   private void verifyOnDirectory(Directory directory, String indexName, int termsAdded, boolean inverted) throws IOException {
      IndexReader indexReader = IndexReader.open(directory);
      IndexSearcher searcher = new IndexSearcher(indexReader);
      try {
         for (int i = 0; i <= termsAdded; i++) {
            String term = String.valueOf(i);
            final Query queryToFind;
            final Query queryNotToFind;
            if (i % 2 == 0 ^ inverted) {
               queryToFind = new TermQuery(new Term("main", term));
               queryNotToFind = new TermQuery(new Term("secondaryField", term));
            }
            else {
               queryToFind = new TermQuery(new Term("secondaryField", term));
               queryNotToFind = new TermQuery(new Term("main", term));
            }
            TopDocs docs = searcher.search(queryToFind, null, 2);
            assert docs.totalHits == 1 : "String '" + term + "' should exist but was not found in index";
            docs = searcher.search(queryNotToFind, null, 1);
            assert docs.totalHits == 0 : "String '" + term + "' should NOT exist but was found in index";
            }
      }
      finally {
         indexReader.close();
      }
   }

   private void createIndex(File rootDir, String indexName, int termsToAdd, boolean invert) throws IOException {
      File indexDir = new File(rootDir, indexName);
      FSDirectory directory = FSDirectory.open(indexDir);
      try {
         CacheTestSupport.initializeDirectory(directory);
         IndexWriter iwriter = LuceneSettings.openWriter(directory, 100000);
         try {
            for (int i = 0; i <= termsToAdd; i++) {
               Document doc = new Document();
               String term = String.valueOf(i);
               //For even values of i we add to "main" field
               if (i % 2 == 0 ^ invert) {
                  doc.add(new Field("main", term, Store.NO, Index.NOT_ANALYZED));
               }
               else {
                  doc.add(new Field("secondaryField", term, Store.YES, Index.NOT_ANALYZED));
               }
               iwriter.addDocument(doc);
            }
            iwriter.commit();
         }
         finally {
            iwriter.close();
         }
      }
      finally {
         directory.close();
      }
   }

}
