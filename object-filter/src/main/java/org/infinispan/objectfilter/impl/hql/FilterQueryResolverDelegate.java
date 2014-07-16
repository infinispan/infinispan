package org.infinispan.objectfilter.impl.hql;

import org.antlr.runtime.tree.Tree;
import org.hibernate.hql.ast.common.JoinType;
import org.hibernate.hql.ast.origin.hql.resolve.path.PathedPropertyReference;
import org.hibernate.hql.ast.origin.hql.resolve.path.PathedPropertyReferenceSource;
import org.hibernate.hql.ast.origin.hql.resolve.path.PropertyPath;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.ast.spi.QueryResolverDelegate;
import org.infinispan.objectfilter.impl.logging.Log;
import org.jboss.logging.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterQueryResolverDelegate implements QueryResolverDelegate {

   private static final Log log = Logger.getMessageLogger(Log.class, FilterQueryResolverDelegate.class.getName());

   private final Map<String, String> aliasToEntityType = new HashMap<String, String>();

   private final ObjectPropertyHelper propertyHelper;

   private final EntityNamesResolver entityNamesResolver;

   private String targetType;

   private boolean definingSelect = false;

   public FilterQueryResolverDelegate(EntityNamesResolver entityNamesResolver, ObjectPropertyHelper propertyHelper) {
      this.entityNamesResolver = entityNamesResolver;
      this.propertyHelper = propertyHelper;
   }

   @Override
   public void registerPersisterSpace(Tree entityNameTree, Tree aliasTree) {
      String alias = aliasTree.getText();
      String entityName = entityNameTree.getText();
      String prevAlias = aliasToEntityType.put(alias, entityName);
      if (prevAlias != null && !prevAlias.equalsIgnoreCase(entityName)) {
         throw new UnsupportedOperationException("Alias reuse currently not supported: aliasTree " + alias + " already assigned to type " + prevAlias);
      }
      if (entityNamesResolver.getClassFromName(entityName) == null) {
         throw new IllegalStateException("Unknown entity name " + entityName);
      }
      if (targetType != null) {
         throw new IllegalStateException("Can't target multiple types: " + targetType + " already selected before " + entityName);
      }
      targetType = entityName;
   }

   @Override
   public boolean isUnqualifiedPropertyReference() {
      return true;
   }

   @Override
   public PathedPropertyReferenceSource normalizeUnqualifiedPropertyReference(Tree property) {
      if (aliasToEntityType.containsKey(property.getText())) {
         return normalizeQualifiedRoot(property);
      }

      return normalizeProperty(new FilterEntityTypeDescriptor(targetType, propertyHelper), Collections.<String>emptyList(), property.getText());
   }

   @Override
   public boolean isPersisterReferenceAlias() {
      return true;
   }

   @Override
   public PathedPropertyReferenceSource normalizeUnqualifiedRoot(Tree identifier382) {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public PathedPropertyReferenceSource normalizeQualifiedRoot(Tree root) {
      String entityNameForAlias = aliasToEntityType.get(root.getText());

      if (entityNameForAlias == null) {
         throw log.getUnknownAliasException(root.getText());
      }

      if (entityNamesResolver.getClassFromName(entityNameForAlias) == null) {
         throw new IllegalStateException("Unknown entity name " + entityNameForAlias);
      }

      return new PathedPropertyReference(root.getText(), new FilterEntityTypeDescriptor(entityNameForAlias, propertyHelper), true);
   }

   @Override
   public PathedPropertyReferenceSource normalizePropertyPathIntermediary(PropertyPath path, Tree propertyName) {

      FilterTypeDescriptor sourceType = (FilterTypeDescriptor) path.getLastNode().getType();

      if (!sourceType.hasProperty(propertyName.getText())) {
         throw log.getNoSuchPropertyException(sourceType.toString(), propertyName.getText());
      }

      List<String> newPath = new LinkedList<String>(path.getNodeNamesWithoutAlias());
      newPath.add(propertyName.getText());

      return new PathedPropertyReference(propertyName.getText(), new FilterEmbeddedEntityTypeDescriptor(sourceType.getEntityType(), newPath, propertyHelper), false);
   }

   @Override
   public PathedPropertyReferenceSource normalizeIntermediateIndexOperation(PathedPropertyReferenceSource propertyReferenceSource, Tree collectionProperty, Tree selector) {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void normalizeTerminalIndexOperation(PathedPropertyReferenceSource propertyReferenceSource, Tree collectionProperty, Tree selector) {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public PathedPropertyReferenceSource normalizeUnqualifiedPropertyReferenceSource(Tree identifier394) {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public PathedPropertyReferenceSource normalizePropertyPathTerminus(PropertyPath path, Tree propertyNameNode) {
      // receives the property name on a specific entity reference _source_
      return normalizeProperty((FilterTypeDescriptor) path.getLastNode().getType(), path.getNodeNamesWithoutAlias(), propertyNameNode.getText());
   }

   private PathedPropertyReferenceSource normalizeProperty(FilterTypeDescriptor type, List<String> path, String propertyName) {
      if (!type.hasProperty(propertyName)) {
         throw log.getNoSuchPropertyException(type.toString(), propertyName);
      }

      if (type.hasEmbeddedProperty(propertyName)) {
         List<String> newPath = new LinkedList<String>(path);
         newPath.add(propertyName);
         return new PathedPropertyReference(propertyName, new FilterEmbeddedEntityTypeDescriptor(type.getEntityType(), newPath, propertyHelper), false);
      }

      return new PathedPropertyReference(propertyName, new FilterPropertyTypeDescriptor(), false);
   }

   @Override
   public void pushFromStrategy(JoinType joinType, Tree associationFetchTree, Tree propertyFetchTree, Tree alias) {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void pushSelectStrategy() {
      definingSelect = true;
   }

   @Override
   public void popStrategy() {
      definingSelect = false;
   }

   @Override
   public void propertyPathCompleted(PropertyPath path) {
      if (definingSelect && path.getLastNode().getType() instanceof FilterEmbeddedEntityTypeDescriptor) {
         FilterEmbeddedEntityTypeDescriptor type = (FilterEmbeddedEntityTypeDescriptor) path.getLastNode().getType();
         throw log.getProjectionOfCompleteEmbeddedEntitiesNotSupportedException(type.getEntityType(), path.asStringPathWithoutAlias());
      }
   }
}
