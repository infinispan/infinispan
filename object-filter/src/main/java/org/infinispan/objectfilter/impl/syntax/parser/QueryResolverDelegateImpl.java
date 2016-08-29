package org.infinispan.objectfilter.impl.syntax.parser;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.tree.Tree;
import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.ql.JoinType;
import org.infinispan.objectfilter.impl.ql.PropertyPath;
import org.infinispan.objectfilter.impl.ql.QueryResolverDelegate;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
final class QueryResolverDelegateImpl<TypeMetadata> implements QueryResolverDelegate<TypeDescriptor<TypeMetadata>> {

   private static final Log log = Logger.getMessageLogger(Log.class, QueryResolverDelegateImpl.class.getName());

   private final Map<String, String> aliasToEntityType = new HashMap<>();

   private final Map<String, PropertyPath<TypeDescriptor<TypeMetadata>>> aliasToPropertyPath = new HashMap<>();

   private final ObjectPropertyHelper<TypeMetadata> propertyHelper;

   private String targetType;

   private TypeMetadata entityMetadata;

   private String alias;

   private enum Phase {
      SELECT,
      FROM
   }

   private Phase phase;

   QueryResolverDelegateImpl(ObjectPropertyHelper<TypeMetadata> propertyHelper) {
      this.propertyHelper = propertyHelper;
   }

   @Override
   public void registerPersisterSpace(String entityName, Tree aliasTree) {
      String aliasText = aliasTree.getText();
      String prevAlias = aliasToEntityType.put(aliasText, entityName);
      if (prevAlias != null && !prevAlias.equalsIgnoreCase(entityName)) {
         throw new UnsupportedOperationException("Alias reuse currently not supported: aliasText " + aliasText + " already assigned to type " + prevAlias);
      }
      if (targetType != null) {
         throw new IllegalStateException("Can't target multiple types: " + targetType + " already selected before " + entityName);
      }
      targetType = entityName;
      entityMetadata = propertyHelper.getEntityMetadata(entityName);
      if (entityMetadata == null) {
         throw log.getUnknownEntity(entityName);
      }
   }

   @Override
   public void registerJoinAlias(Tree alias, PropertyPath<TypeDescriptor<TypeMetadata>> path) {
      if (!path.isEmpty() && !aliasToPropertyPath.containsKey(alias.getText())) {
         aliasToPropertyPath.put(alias.getText(), path);
      }
   }

   @Override
   public boolean isUnqualifiedPropertyReference() {
      return true;
   }

   @Override
   public PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>> normalizeUnqualifiedPropertyReference(Tree propertyNameTree) {
      String property = propertyNameTree.getText();
      if (aliasToEntityType.containsKey(property)) {
         return normalizeQualifiedRoot(propertyNameTree);
      }

      EntityTypeDescriptor<TypeMetadata> type = new EntityTypeDescriptor<>(targetType, entityMetadata);
      return normalizeProperty(type, Collections.emptyList(), property);
   }

   @Override
   public boolean isPersisterReferenceAlias() {
      return aliasToEntityType.containsKey(alias);
   }

   @Override
   public PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>> normalizeUnqualifiedRoot(Tree aliasTree) {
      String alias = aliasTree.getText();
      if (aliasToEntityType.containsKey(alias)) {
         return normalizeQualifiedRoot(aliasTree);
      }

      PropertyPath<TypeDescriptor<TypeMetadata>> propertyPath = aliasToPropertyPath.get(alias);
      if (propertyPath == null) {
         throw log.getUnknownAliasException(alias);
      }

      List<String> resolvedAlias = resolveAlias(propertyPath);
      TypeDescriptor<TypeMetadata> sourceType = propertyPath.getFirst().getTypeDescriptor();

      EmbeddedEntityTypeDescriptor<TypeMetadata> type = new EmbeddedEntityTypeDescriptor<>(sourceType.getTypeName(), sourceType.getTypeMetadata(), resolvedAlias);
      return new PropertyPath.PropertyReference<>(alias, type, true);
   }

   @Override
   public PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>> normalizeQualifiedRoot(Tree root) {
      String alias = root.getText();
      String entityNameForAlias = aliasToEntityType.get(alias);

      if (entityNameForAlias == null) {
         PropertyPath<TypeDescriptor<TypeMetadata>> propertyPath = aliasToPropertyPath.get(alias);
         if (propertyPath == null) {
            throw log.getUnknownAliasException(alias);
         }
         return new PropertyPath.PropertyReference<>(propertyPath.asStringPathWithoutAlias(), null, false);
      }

      TypeMetadata entityMetadata = propertyHelper.getEntityMetadata(entityNameForAlias);
      if (entityMetadata == null) {
         throw log.getUnknownEntity(entityNameForAlias);
      }

      return new PropertyPath.PropertyReference<>(alias, new EntityTypeDescriptor<>(entityNameForAlias, entityMetadata), true);
   }

   @Override
   public PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>> normalizePropertyPathIntermediary(PropertyPath<TypeDescriptor<TypeMetadata>> path, Tree propertyNameTree) {
      String propertyName = propertyNameTree.getText();
      TypeDescriptor<TypeMetadata> sourceType = path.getLast().getTypeDescriptor();
      if (!propertyHelper.hasProperty(sourceType.getTypeMetadata(), sourceType.makePath(propertyName))) {
         throw log.getNoSuchPropertyException(sourceType.getTypeName(), propertyName);
      }

      List<String> newPath = resolveAlias(path);
      newPath.add(propertyName);
      EmbeddedEntityTypeDescriptor<TypeMetadata> type = new EmbeddedEntityTypeDescriptor<>(sourceType.getTypeName(), sourceType.getTypeMetadata(), newPath);
      return new PropertyPath.PropertyReference<>(propertyName, type, false);
   }

   private List<String> resolveAlias(PropertyPath<TypeDescriptor<TypeMetadata>> path) {
      if (path.isAlias()) {
         String alias = path.getFirst().getPropertyName();
         if (aliasToEntityType.containsKey(alias)) {
            // Alias for entity
            return path.getNodeNamesWithoutAlias();
         } else if (aliasToPropertyPath.containsKey(alias)) {
            // Alias for embedded
            PropertyPath<TypeDescriptor<TypeMetadata>> propertyPath = aliasToPropertyPath.get(alias);
            List<String> resolvedAlias = resolveAlias(propertyPath);
            resolvedAlias.addAll(path.getNodeNamesWithoutAlias());
            return resolvedAlias;
         } else {
            // Alias not found
            throw log.getUnknownAliasException(alias);
         }
      }
      // It does not start with an alias
      return path.getNodeNamesWithoutAlias();
   }

   @Override
   public PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>> normalizeIntermediateIndexOperation(PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>> propertyReference, Tree collectionProperty, Tree selector) {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void normalizeTerminalIndexOperation(PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>> propertyReference, Tree collectionProperty, Tree selector) {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>> normalizeUnqualifiedPropertyReferenceSource(Tree identifier) {
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>> normalizePropertyPathTerminus(PropertyPath<TypeDescriptor<TypeMetadata>> path, Tree propertyNameTree) {
      // receives the property name on a specific entity reference _source_
      return normalizeProperty(path.getLast().getTypeDescriptor(), path.getNodeNamesWithoutAlias(), propertyNameTree.getText());
   }

   private PropertyPath.PropertyReference<TypeDescriptor<TypeMetadata>> normalizeProperty(TypeDescriptor<TypeMetadata> type, List<String> path, String propertyName) {
      String[] propertyPath = type.makePath(propertyName);
      if (!propertyHelper.hasProperty(type.getTypeMetadata(), propertyPath)) {
         throw log.getNoSuchPropertyException(type.getTypeName(), propertyName);
      }

      TypeDescriptor<TypeMetadata> propType = null;
      if (propertyHelper.hasEmbeddedProperty(type.getTypeMetadata(), propertyPath)) {
         List<String> newPath = new LinkedList<>(path);
         newPath.add(propertyName);
         propType = new EmbeddedEntityTypeDescriptor<>(type.getTypeName(), type.getTypeMetadata(), newPath);
      }
      return new PropertyPath.PropertyReference<>(propertyName, propType, false);
   }

   @Override
   public void activateFromStrategy(JoinType joinType, Tree associationFetchTree, Tree propertyFetchTree, Tree aliasTree) {
      phase = Phase.FROM;
      alias = aliasTree.getText();
   }

   @Override
   public void activateSelectStrategy() {
      phase = Phase.SELECT;
   }

   @Override
   public void deactivateStrategy() {
      phase = null;
      alias = null;
   }

   @Override
   public void propertyPathCompleted(PropertyPath<TypeDescriptor<TypeMetadata>> path) {
      if (phase == Phase.SELECT) {
         TypeDescriptor<TypeMetadata> type = path.getLast().getTypeDescriptor();
         if (type instanceof EmbeddedEntityTypeDescriptor<?>) {
            throw log.getProjectionOfCompleteEmbeddedEntitiesNotSupportedException(type.getTypeName(), path.asStringPathWithoutAlias());
         }
      }
   }
}
