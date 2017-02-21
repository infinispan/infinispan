package org.infinispan.query.backend;

import org.hibernate.search.analyzer.definition.spi.LuceneAnalyzerDefinitionProvider;
import org.hibernate.search.analyzer.definition.spi.LuceneAnalyzerDefinitionSourceService;

public class LuceneAnalyzerDefinitionsBuilderService implements LuceneAnalyzerDefinitionSourceService {

   private final LuceneAnalyzerDefinitionProvider defsProvider;

   LuceneAnalyzerDefinitionsBuilderService(LuceneAnalyzerDefinitionProvider defsProvider) {
      this.defsProvider = defsProvider;
   }

   @Override
   public LuceneAnalyzerDefinitionProvider getLuceneAnalyzerDefinitionProvider() {
      return defsProvider;
   }

}
