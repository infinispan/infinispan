package org.infinispan.query.remote.impl;

import org.apache.lucene.analysis.Analyzer;
import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.query.dsl.EntityContext;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.objectfilter.impl.syntax.AndExpr;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.ComparisonExpr;
import org.infinispan.objectfilter.impl.syntax.ExprVisitor;
import org.infinispan.objectfilter.impl.syntax.FullTextBoostExpr;
import org.infinispan.objectfilter.impl.syntax.FullTextOccurExpr;
import org.infinispan.objectfilter.impl.syntax.FullTextRangeExpr;
import org.infinispan.objectfilter.impl.syntax.FullTextRegexpExpr;
import org.infinispan.objectfilter.impl.syntax.FullTextTermExpr;
import org.infinispan.objectfilter.impl.syntax.IsNullExpr;
import org.infinispan.objectfilter.impl.syntax.LikeExpr;
import org.infinispan.objectfilter.impl.syntax.NotExpr;
import org.infinispan.objectfilter.impl.syntax.OrExpr;
import org.infinispan.objectfilter.impl.syntax.PropertyValueExpr;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.dsl.embedded.impl.LuceneQueryMaker;
import org.infinispan.query.remote.impl.indexing.FieldMapping;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;
import org.infinispan.query.remote.impl.logging.Log;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
final class ProtobufFieldBridgeAndAnalyzerProvider implements LuceneQueryMaker.FieldBridgeAndAnalyzerProvider<Descriptor> {

   private static final Log log = LogFactory.getLog(ProtobufFieldBridgeAndAnalyzerProvider.class, Log.class);

   ProtobufFieldBridgeAndAnalyzerProvider() {
   }

   @Override
   public FieldBridge getFieldBridge(Descriptor typeMetadata, String[] propertyPath) {
      FieldDescriptor fd = getFieldDescriptor(typeMetadata, propertyPath);
      IndexingMetadata indexingMetadata = fd.getContainingMessage().getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
      FieldMapping fieldMapping = indexingMetadata != null ? indexingMetadata.getFieldMapping(fd.getName()) : null;

      if (fieldMapping != null) {
         return fieldMapping.fieldBridge();
      }

      return FieldMapping.getDefaultFieldBridge(fd.getType());
   }

   private FieldDescriptor getFieldDescriptor(Descriptor typeMetadata, String[] propertyPath) {
      Descriptor messageDescriptor = typeMetadata;
      FieldDescriptor fd = null;
      for (int i = 0; i < propertyPath.length; i++) {
         String name = propertyPath[i];
         fd = messageDescriptor.findFieldByName(name);
         if (fd == null) {
            throw log.unknownField(name, messageDescriptor.getFullName());
         }
         IndexingMetadata indexingMetadata = messageDescriptor.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
         if (indexingMetadata != null && !indexingMetadata.isFieldIndexed(name)) {
            throw log.fieldIsNotIndexed(name, messageDescriptor.getFullName());
         }
         if (i < propertyPath.length - 1) {
            messageDescriptor = fd.getMessageType();
         }
      }
      return fd;
   }

   @Override
   public Analyzer getAnalyzer(SearchIntegrator searchIntegrator, Descriptor typeMetadata, String[] propertyPath) {
      String analyzerName = getAnalyzerName(typeMetadata, propertyPath, true);
      return analyzerName != null ? searchIntegrator.getAnalyzer(analyzerName) : null;
   }

   private String getAnalyzerName(Descriptor typeMetadata, String[] propertyPath, boolean complain) {
      Descriptor messageDescriptor = typeMetadata;
      for (int i = 0; i < propertyPath.length; i++) {
         String name = propertyPath[i];
         FieldDescriptor fd = messageDescriptor.findFieldByName(name);
         if (fd == null) {
            throw log.unknownField(name, messageDescriptor.getFullName());
         }
         IndexingMetadata indexingMetadata = messageDescriptor.getProcessedAnnotation(IndexingMetadata.INDEXED_ANNOTATION);
         if (indexingMetadata == null || !indexingMetadata.isFieldIndexed(name)) {
            if (complain) {
               throw log.fieldIsNotIndexed(name, messageDescriptor.getFullName());
            } else {
               break;
            }
         }
         if (i < propertyPath.length - 1) {
            messageDescriptor = fd.getMessageType();
         } else {
            FieldMapping fieldMapping = indexingMetadata.getFieldMapping(name);
            if (fieldMapping == null) {
               if (complain) {
                  throw log.fieldIsNotAnalyzed(name, messageDescriptor.getFullName());
               } else {
                  break;
               }
            }
            String analyzerName = fieldMapping.analyzer();
            if (analyzerName == null || analyzerName.isEmpty()) {
               analyzerName = indexingMetadata.analyzer();
            }
            if (analyzerName != null && !analyzerName.isEmpty()) {
               return analyzerName;
            }
         }
      }
      return null;
   }

   @Override
   public void overrideAnalyzers(IckleParsingResult<Descriptor> parsingResult, EntityContext entityContext) {

      // Visit an expression tree and collect analyzers of all analyzed properties
      class AnalyzerCollector extends ExprVisitor {

         private void collectAnalyzer(PropertyValueExpr propertyValueExpr) {
            String analyzerName = getAnalyzerName(parsingResult.getTargetEntityMetadata(), propertyValueExpr.getPropertyPath().asArrayPath(), false);
            if (analyzerName != null) {
               entityContext.overridesForField(propertyValueExpr.getPropertyPath().asStringPathWithoutAlias(), analyzerName);
            }
         }

         @Override
         public BooleanExpr visit(FullTextBoostExpr fullTextBoostExpr) {
            fullTextBoostExpr.getChild().acceptVisitor(this);
            return fullTextBoostExpr;
         }

         @Override
         public BooleanExpr visit(FullTextOccurExpr fullTextOccurExpr) {
            fullTextOccurExpr.getChild().acceptVisitor(this);
            return fullTextOccurExpr;
         }

         @Override
         public BooleanExpr visit(FullTextTermExpr fullTextTermExpr) {
            PropertyValueExpr propertyValueExpr = (PropertyValueExpr) fullTextTermExpr.getChild();
            collectAnalyzer(propertyValueExpr);
            return fullTextTermExpr;
         }

         @Override
         public BooleanExpr visit(FullTextRegexpExpr fullTextRegexpExpr) {
            PropertyValueExpr propertyValueExpr = (PropertyValueExpr) fullTextRegexpExpr.getChild();
            collectAnalyzer(propertyValueExpr);
            return fullTextRegexpExpr;
         }

         @Override
         public BooleanExpr visit(FullTextRangeExpr fullTextRangeExpr) {
            PropertyValueExpr propertyValueExpr = (PropertyValueExpr) fullTextRangeExpr.getChild();
            collectAnalyzer(propertyValueExpr);
            return fullTextRangeExpr;
         }

         @Override
         public BooleanExpr visit(NotExpr notExpr) {
            notExpr.getChild().acceptVisitor(this);
            return notExpr;
         }

         @Override
         public BooleanExpr visit(OrExpr orExpr) {
            for (BooleanExpr c : orExpr.getChildren()) {
               c.acceptVisitor(this);
            }
            return orExpr;
         }

         @Override
         public BooleanExpr visit(AndExpr andExpr) {
            for (BooleanExpr c : andExpr.getChildren()) {
               c.acceptVisitor(this);
            }
            return andExpr;
         }

         @Override
         public BooleanExpr visit(IsNullExpr isNullExpr) {
            PropertyValueExpr propertyValueExpr = (PropertyValueExpr) isNullExpr.getChild();
            collectAnalyzer(propertyValueExpr);
            return isNullExpr;
         }

         @Override
         public BooleanExpr visit(ComparisonExpr comparisonExpr) {
            PropertyValueExpr propertyValueExpr = (PropertyValueExpr) comparisonExpr.getLeftChild();
            collectAnalyzer(propertyValueExpr);
            return comparisonExpr;
         }

         @Override
         public BooleanExpr visit(LikeExpr likeExpr) {
            PropertyValueExpr propertyValueExpr = (PropertyValueExpr) likeExpr.getChild();
            collectAnalyzer(propertyValueExpr);
            return likeExpr;
         }
      }

      if (parsingResult.getWhereClause() != null) {
         parsingResult.getWhereClause().acceptVisitor(new AnalyzerCollector());
      }
      if (parsingResult.getHavingClause() != null) {
         parsingResult.getHavingClause().acceptVisitor(new AnalyzerCollector());
      }
   }
}
