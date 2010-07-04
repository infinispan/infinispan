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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.infinispan.config.Configuration;
import org.infinispan.lucene.testutils.LuceneSettings;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * SimpleLuceneTest tests the basic functionality of the Lucene Directory
 * on Infinispan: what is inserted in one node should be able to be found in
 * a second node.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
@Test(groups = "functional", testName = "lucene.SimpleLuceneTest")
public class SimpleLuceneTest extends MultipleCacheManagersTest {
   
   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration configuration = CacheTestSupport.createTestConfiguration();
      createClusteredCaches(2, "lucene", configuration);
   }
   
   @Test
   @SuppressWarnings("unchecked")
   public void testIndexWritingAndFinding() throws IOException {
      Directory dirA = new InfinispanDirectory(cache(0, "lucene"), "indexName");
      Directory dirB = new InfinispanDirectory(cache(1, "lucene"), "indexName");
      writeTextToIndex(dirA, 0, "hi from node A");
      assertTextIsFoundInIds(dirA, "hi", 0);
      assertTextIsFoundInIds(dirB, "hi", 0);
      writeTextToIndex(dirB, 1, "hello node A, how are you?");
      assertTextIsFoundInIds(dirA, "hello", 1);
      assertTextIsFoundInIds(dirB, "hello", 1);
      assertTextIsFoundInIds(dirA, "node", 1, 0); // node is keyword in both documents id=0 and id=1
      assertTextIsFoundInIds(dirB, "node", 1, 0);
      removeByTerm(dirA, "from");
      assertTextIsFoundInIds(dirB, "node", 1);
      dirA.close();
      dirB.close();
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache(0, "lucene"), "indexName");
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache(1, "lucene"), "indexName");
   }
   
   @Test(description="Verifies the caches can be reused after a Directory close")
   @SuppressWarnings("unchecked")
   public void testCacheReuse() throws IOException {
      testIndexWritingAndFinding();
      cache(0, "lucene").clear();
      testIndexWritingAndFinding();
   }


   /**
    * Used in test to remove all documents containing some term
    * 
    * @param dir The Directory containing the Index to verify
    * @param string
    */
   private void removeByTerm(Directory dir, String term) throws IOException {
      IndexWriter iw = new IndexWriter(dir, LuceneSettings.analyzer, IndexWriter.MaxFieldLength.UNLIMITED);
      iw.deleteDocuments(new Term("body", term));
      iw.commit();
      iw.close();
   }

   /**
    * Used in test to verify an Index
    * 
    * @param dir The Directory containing the Index to verify
    * @param term a single Term (after analysis) to be searched for
    * @param validDocumentIds The list of document identifiers which should contain the searched-for term
    * @throws IOException
    */
   private void assertTextIsFoundInIds(Directory dir, String term, Integer... validDocumentIds) throws IOException {
      int expectedResults = validDocumentIds.length;
      Set<Integer> expectedDocumendIds = new HashSet<Integer>(Arrays.asList(validDocumentIds));
      IndexSearcher searcher = new IndexSearcher(dir,true);
      Query query = new TermQuery(new Term("body", term));
      TopDocs docs = searcher.search(query, null, expectedResults + 1);
      assert docs.totalHits == expectedResults;
      for (ScoreDoc scoreDoc : docs.scoreDocs) {
         int docId = scoreDoc.doc;
         Document document = searcher.doc(docId);
         String idString = document.get("id");
         assert idString != null;
         Integer idFoundElement = Integer.valueOf(idString);
         assert expectedDocumendIds.contains(idFoundElement);
      }
      searcher.close();
   }

   /**
    * Used in test to add a new Document to an Index; two fields are created: id and body 
    * 
    * @param dir The Directory containing the Index to modify
    * @param id a sequential number to identify this document (id field)
    * @param text Some text to add to the body field
    * @throws IOException
    */
   private void writeTextToIndex(Directory dir, int id, String text) throws IOException {
      IndexWriter iw = new IndexWriter(dir, LuceneSettings.analyzer, IndexWriter.MaxFieldLength.UNLIMITED);
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(id), Field.Store.YES, Field.Index.NOT_ANALYZED));
      doc.add(new Field("body", text, Field.Store.NO, Field.Index.ANALYZED));
      iw.addDocument(doc);
      iw.commit();
      iw.close();
   }

}
