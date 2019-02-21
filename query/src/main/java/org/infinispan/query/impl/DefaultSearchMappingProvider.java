package org.infinispan.query.impl;

import java.util.Map;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizerFactory;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.LowerCaseTokenizerFactory;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.core.WhitespaceTokenizerFactory;
import org.apache.lucene.analysis.ngram.NGramFilterFactory;
import org.apache.lucene.analysis.snowball.SnowballPorterFilterFactory;
import org.apache.lucene.analysis.standard.StandardFilterFactory;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;
import org.hibernate.search.cfg.SearchMapping;
import org.infinispan.Cache;
import org.infinispan.query.spi.ProgrammaticSearchMappingProvider;

/**
 * Defines default analyzers.
 *
 * @since 9.3.2
 */
public class DefaultSearchMappingProvider implements ProgrammaticSearchMappingProvider {
   @Override
   public void defineMappings(Cache cache, SearchMapping searchMapping) {
      searchMapping
            .analyzerDef("standard", StandardTokenizerFactory.class)
              .filter(StandardFilterFactory.class)
              .filter(LowerCaseFilterFactory.class)
            .analyzerDef("simple", LowerCaseTokenizerFactory.class)
              .filter(LowerCaseFilterFactory.class)
            .analyzerDef("whitespace", WhitespaceTokenizerFactory.class)
            .analyzerDef("keyword", KeywordTokenizerFactory.class)
            .analyzerDef("stemmer", StandardTokenizerFactory.class)
              .filter(StandardFilterFactory.class)
              .filter(LowerCaseFilterFactory.class)
              .filter(StopFilterFactory.class)
              .filter(SnowballPorterFilterFactory.class)
                .param("language", "English")
            .analyzerDef("ngram", StandardTokenizerFactory.class)
              .filter(StandardFilterFactory.class)
              .filter(LowerCaseFilterFactory.class)
              .filter(StopFilterFactory.class)
              .filter(NGramFilterFactory.class)
                .param("minGramSize", "3")
                .param("maxGramSize", "3")
            .analyzerDef("filename", ConfigurableBufferSizeKeywordTokenizerFactory.class)
               .tokenizerParam("bufferSize", "2048")
               .filter(StandardFilterFactory.class)
               .filter(LowerCaseFilterFactory.class);
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
