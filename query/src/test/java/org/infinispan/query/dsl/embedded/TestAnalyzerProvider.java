package org.infinispan.query.dsl.embedded;

import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.StopFilterFactory;
import org.apache.lucene.analysis.ngram.NGramFilterFactory;
import org.apache.lucene.analysis.snowball.SnowballPorterFilterFactory;
import org.apache.lucene.analysis.standard.StandardFilterFactory;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.hibernate.search.cfg.SearchMapping;
import org.infinispan.Cache;
import org.infinispan.query.spi.ProgrammaticSearchMappingProvider;
import org.kohsuke.MetaInfServices;

/**
 * Enrich the search mapping configuration with several analyzer definitions to be used by query tests.
 *
 * @author anistor@redhat.com
 */
@MetaInfServices
@SuppressWarnings("unused")
public final class TestAnalyzerProvider implements ProgrammaticSearchMappingProvider {

   @Override
   public void defineMappings(Cache cache, SearchMapping searchMapping) {
      searchMapping
            .analyzerDef("standard", StandardTokenizerFactory.class)
               .filter(StandardFilterFactory.class)
               .filter(LowerCaseFilterFactory.class)
               .filter(StopFilterFactory.class)
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
