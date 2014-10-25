package org.infinispan.lucene.testutils;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

/**
 * Collects common LuceneSettings for all tests; especially define the backwards compatibility.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
public class LuceneSettings {

   public static final Version LUCENE_VERSION = Version.LUCENE_4_10_1;

   public static final Analyzer analyzer = new SimpleAnalyzer();

   private static final MergeScheduler mergeScheduler = new SerialMergeScheduler();

   private LuceneSettings() {
      //Not to be instantiated
   }

   public static IndexWriter openWriter(Directory directory, int maxMergeDocs, boolean useSerialMerger) throws IOException {
      IndexWriterConfig indexWriterConfig = new IndexWriterConfig(LUCENE_VERSION, analyzer);
      if (useSerialMerger) {
         indexWriterConfig.setMergeScheduler(mergeScheduler);
      }
      LogMergePolicy mergePolicy = new LogByteSizeMergePolicy();
      mergePolicy.setMaxMergeDocs(maxMergeDocs);
      indexWriterConfig.setMergePolicy(mergePolicy);
      return new IndexWriter(directory, indexWriterConfig);
   }

   public static IndexWriter openWriter(Directory directory, int maxMergeDocs) throws IOException {
      return openWriter(directory, maxMergeDocs, false);
   }

}
