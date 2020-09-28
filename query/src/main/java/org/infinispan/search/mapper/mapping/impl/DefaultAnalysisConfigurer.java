package org.infinispan.search.mapper.mapping.impl;

import java.util.Map;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizerFactory;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.core.WhitespaceTokenizerFactory;
import org.apache.lucene.analysis.ngram.NGramTokenizerFactory;
import org.apache.lucene.analysis.snowball.SnowballPorterFilterFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;
import org.hibernate.search.backend.lucene.analysis.LuceneAnalysisConfigurationContext;
import org.hibernate.search.backend.lucene.analysis.LuceneAnalysisConfigurer;

public class DefaultAnalysisConfigurer implements LuceneAnalysisConfigurer {

   public static final String STANDARD_ANALYZER_NAME = "standard";

   @Override
   public void configure(LuceneAnalysisConfigurationContext context) {
      context.analyzer(STANDARD_ANALYZER_NAME)
            .instance(new StandardAnalyzer());

      context.analyzer("simple").custom()
            .tokenizer(StandardTokenizerFactory.class)
            .tokenFilter(LowerCaseFilterFactory.class);

      context.analyzer("whitespace").custom()
            .tokenizer(WhitespaceTokenizerFactory.class);

      context.analyzer("keyword").custom()
            .tokenizer(KeywordTokenizerFactory.class);

      context.analyzer("stemmer").custom()
            .tokenizer(StandardTokenizerFactory.class)
            .tokenFilter(LowerCaseFilterFactory.class)
            .tokenFilter(StopFilterFactory.class)
            .tokenFilter(SnowballPorterFilterFactory.class)
               .param("language", "English");

      context.analyzer("ngram").custom()
            .tokenizer(NGramTokenizerFactory.class)
               .param("minGramSize", "3")
               .param("maxGramSize", "3")
            .tokenFilter(LowerCaseFilterFactory.class)
            .tokenFilter(StopFilterFactory.class);

      context.analyzer("filename").custom()
            .tokenizer(ConfigurableBufferSizeKeywordTokenizerFactory.class)
               .param("bufferSize", "2048")
            .tokenFilter(LowerCaseFilterFactory.class);
   }

   /**
    * Similar to {@link KeywordTokenizerFactory} but with a configurable buffer size. This tokenizer factory accepts an
    * integer param named <code>"bufferSize"</code> that defaults to <code>256</code>.
    */
   public static final class ConfigurableBufferSizeKeywordTokenizerFactory extends TokenizerFactory {

      private final int bufferSize;

      public ConfigurableBufferSizeKeywordTokenizerFactory(Map<String, String> args) {
         super(args);
         bufferSize = getInt(args, "bufferSize", KeywordTokenizer.DEFAULT_BUFFER_SIZE);
         if (!args.isEmpty()) {
            throw new IllegalArgumentException("Unknown parameters: " + args);
         }
      }

      @Override
      public Tokenizer create(AttributeFactory factory) {
         return new KeywordTokenizer(factory, bufferSize);
      }
   }
}
