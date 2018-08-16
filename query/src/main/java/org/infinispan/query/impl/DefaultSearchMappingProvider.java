package org.infinispan.query.impl;

import org.apache.lucene.analysis.core.KeywordTokenizerFactory;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.LowerCaseTokenizerFactory;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.core.WhitespaceTokenizerFactory;
import org.apache.lucene.analysis.ngram.NGramFilterFactory;
import org.apache.lucene.analysis.snowball.SnowballPorterFilterFactory;
import org.apache.lucene.analysis.standard.StandardFilterFactory;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
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
                .param("maxGramSize", "3");
   }
}
