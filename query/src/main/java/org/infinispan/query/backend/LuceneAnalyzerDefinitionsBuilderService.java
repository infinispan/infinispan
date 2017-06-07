package org.infinispan.query.backend;

import org.hibernate.search.analyzer.definition.LuceneAnalysisDefinitionProvider;
import org.hibernate.search.analyzer.definition.spi.LuceneAnalysisDefinitionSourceService;

public class LuceneAnalyzerDefinitionsBuilderService implements LuceneAnalysisDefinitionSourceService {

   private final LuceneAnalysisDefinitionProvider defsProvider;

   LuceneAnalyzerDefinitionsBuilderService(LuceneAnalysisDefinitionProvider defsProvider) {
      this.defsProvider = defsProvider;
   }

   @Override
   public LuceneAnalysisDefinitionProvider getLuceneAnalyzerDefinitionProvider() {
      return defsProvider;
   }

}
