/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
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

package org.infinispan.lucene.cachestore;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.testutils.LuceneSettings;
import org.testng.AssertJUnit;

import java.io.File;
import java.io.IOException;

/**
 * Helper class which will contain common using methods.
 *
 * @author Anna Manukyan
 */
public class TestHelper {
   /**
    * Verifies that entries exist/not exist in the index.
    *
    * @param rootDir       index root directory.
    * @param indexName     index name.
    * @param termsAdded    number of terms to be verified.
    * @param inverted      flag to define which term should exist which not.
    * @throws IOException
    */
   public static void verifyIndex(File rootDir, String indexName, int termsAdded, boolean inverted) throws IOException {
      File indexDir = new File(rootDir, indexName);
      Directory directory = FSDirectory.open(indexDir);
      try {
         verifyOnDirectory(directory, termsAdded, inverted);
      }
      finally {
         directory.close();
      }
   }

   /**
    * Verifies that entries exist/not exist in the index.
    *
    * @param directory        index root directory.
    * @param termsAdded       number of terms to be verified.
    * @param inverted         flag to define which term should exist which not.
    * @throws IOException
    */
   public static void verifyOnDirectory(Directory directory, int termsAdded, boolean inverted) throws IOException {
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
            AssertJUnit.assertEquals("String '" + term + "' should exist but was not found in index", 1, docs.totalHits);
            docs = searcher.search(queryNotToFind, null, 1);
            AssertJUnit.assertEquals("String '" + term + "' should NOT exist but was found in index", 0, docs.totalHits);
         }
      }
      finally {
         indexReader.close();
      }
   }

   /**
    * Creates terms and inserts them into the index.
    *
    * @param rootDir          index root directory.
    * @param indexName        the name of the index.
    * @param termsToAdd       number of terms to be added.
    * @param invert           flag which identifies which terms should be inserted which not.
    * @throws IOException
    */
   public static void createIndex(File rootDir, String indexName, int termsToAdd, boolean invert) throws IOException {
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
                  doc.add(new Field("main", term, Field.Store.NO, Field.Index.NOT_ANALYZED));
               }
               else {
                  doc.add(new Field("secondaryField", term, Field.Store.YES, Field.Index.NOT_ANALYZED));
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

   /**
    * Creates root directory with subdirectories based on the provided directory name and it's sub directory name.
    *
    * @param parentDir              the path of the root directory.
    * @param rootDirectoryName      the path of the sub directory.
    * @return                       the created file.
    */
   public static File createRootDir(final String parentDir, final String rootDirectoryName) {
      File rootDir = new File(new File(parentDir).getAbsoluteFile(), rootDirectoryName);
      boolean directoriesCreated = rootDir.mkdir();
      assert directoriesCreated : "couldn't create directory for test";

      rootDir.deleteOnExit();

      return rootDir;
   }

   /**
    * Returns the list of file names in the specified directory.
    *
    * @param rootDir       the root directory.
    * @param indexName     the name of the subdirectory.
    * @return              the array of the names of the files.
    */
   public static String[] getFileNamesFromDir(File rootDir, String indexName) {
      File indexDir = new File(rootDir.getAbsoluteFile(), indexName);
      assert indexDir.exists();

      String[] fileNames = indexDir.list();
      assert fileNames.length > 0;

      return fileNames;
   }
}
