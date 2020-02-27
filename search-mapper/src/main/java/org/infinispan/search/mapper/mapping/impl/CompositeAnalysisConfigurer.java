package org.infinispan.search.mapper.mapping.impl;

import java.util.Collection;

import org.hibernate.search.backend.lucene.analysis.LuceneAnalysisConfigurationContext;
import org.hibernate.search.backend.lucene.analysis.LuceneAnalysisConfigurer;

public class CompositeAnalysisConfigurer implements LuceneAnalysisConfigurer {

   private final Collection<LuceneAnalysisConfigurer> children;

   public CompositeAnalysisConfigurer(Collection<LuceneAnalysisConfigurer> children) {
      this.children = children;
   }

   @Override
   public void configure(LuceneAnalysisConfigurationContext luceneAnalysisConfigurationContext) {
      for (LuceneAnalysisConfigurer child : children) {
         child.configure(luceneAnalysisConfigurationContext);
      }
   }
}
