package org.infinispan.objectfilter.impl.hql;

import org.antlr.runtime.tree.Tree;
import org.hibernate.hql.ast.common.JoinType;
import org.hibernate.hql.ast.origin.hql.resolve.path.PathedPropertyReference;
import org.hibernate.hql.ast.origin.hql.resolve.path.PathedPropertyReferenceSource;
import org.hibernate.hql.ast.origin.hql.resolve.path.PropertyPath;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.ast.spi.QueryResolverDelegate;
import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.util.StringHelper;
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
public class FilterQueryResolverDelegate implements QueryResolverDelegate {

   private static final Log log = Logger.getMessageLogger(Log.class, FilterQueryResolverDelegate.class.getName());

   protected final Map<String, String> aliasToEntityType = new HashMap<>();

   protected final Map<String, PropertyPath> aliasToPropertyPath = new HashMap<>();

   protected final ObjectPropertyHelper propertyHelper;

   protected final EntityNamesResolver entityNamesResolver;

   protected String targetType;

   protected Class<?> targetClass;

   protected String alias;

   protected enum Status {
      DEFINING_SELECT, DEFINING_FROM
   }

   protected Status status;

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
         throw new UnsupportedOperationException("Alias reuse currently not supported: alias " + alias + " already assigned to type " + prevAlias);
      }
      if (targetType != null) {
         throw new IllegalStateException("Can't target multiple types: " + targetType + " already selected before " + entityName);
      }
      targetType = entityName;
      targetClass = entityNamesResolver.getClassFromName(entityName);
      if (targetClass == null) {
         throw new IllegalStateException("Unknown entity name " + entityName);
      }
   }

   @Override
   public void registerJoinAlias(Tree alias, PropertyPath path) {
      if (!path.getNodes().isEmpty() && !aliasToPropertyPath.containsKey(alias.getText())) {
         aliasToPropertyPath.put(alias.getText(), path);
      }
   }

   @Override
   public boolean isUnqualifiedPropertyReference() {
      return true;
   }

   @Override
   public PathedPropertyReferenceSource normalizeUnqualifiedPropertyReference(Tree propertyTree) {
      String property = propertyTree.getText();
      if (aliasToEntityType.containsKey(property)) {
         return normalizeQualifiedRoot(propertyTree);
      }

      return normalizeProperty(new FilterEntityTypeDescriptor(targetType, propertyHelper), Collections.emptyList(), property);
   }

   @Override
   public boolean isPersisterReferenceAlias() {
      return aliasToEntityType.containsKey(alias);
   }

   @Override
   public PathedPropertyReferenceSource normalizeUnqualifiedRoot(Tree identifier382) {
      String aliasText = identifier382.getText();
      if (aliasToEntityType.containsKey(aliasText)) {
         return normalizeQualifiedRoot(identifier382);
      }
      if (aliasToPropertyPath.containsKey(aliasText)) {
         PropertyPath propertyPath = aliasToPropertyPath.get(aliasText);
         if (propertyPath == null) {
            throw log.getUnknownAliasException(aliasText);
         }
         FilterTypeDescriptor sourceType = (FilterTypeDescriptor) propertyPath.getNodes().get(0).getType();
         List<String> resolveAlias = resolveAlias(propertyPath);

         return new PathedPropertyReference(aliasText, new FilterEmbeddedEntityTypeDescriptor(sourceType.getEntityType(), resolveAlias, propertyHelper), true);
      }
      throw log.getUnknownAliasException(aliasText);
   }

   @Override
   public PathedPropertyReferenceSource normalizeQualifiedRoot(Tree root) {
      String alias = root.getText();
      String entityNameForAlias = aliasToEntityType.get(alias);

      if (entityNameForAlias == null) {
         PropertyPath propertyPath = aliasToPropertyPath.get(alias);
         if (propertyPath == null) {
            throw log.getUnknownAliasException(alias);
         }
         return new PathedPropertyReference(StringHelper.join(propertyPath.getNodeNamesWithoutAlias()), null, false);
      }

      if (entityNamesResolver.getClassFromName(entityNameForAlias) == null) {
         throw new IllegalStateException("Unknown entity name " + entityNameForAlias);
      }

      return new PathedPropertyReference(alias, new FilterEntityTypeDescriptor(entityNameForAlias, propertyHelper), true);
   }

   @Override
   public PathedPropertyReferenceSource normalizePropertyPathIntermediary(PropertyPath path, Tree propertyNameTree) {
      FilterTypeDescriptor sourceType = (FilterTypeDescriptor) path.getLastNode().getType();
      String propertyName = propertyNameTree.getText();
      if (!sourceType.hasProperty(propertyName)) {
         throw log.getNoSuchPropertyException(sourceType.toString(), propertyName);
      }

      List<String> newPath = resolveAlias(path);
      newPath.add(propertyName);
      return new PathedPropertyReference(propertyName, new FilterEmbeddedEntityTypeDescriptor(sourceType.getEntityType(), newPath, propertyHelper), false);
   }

   private List<String> resolveAlias(PropertyPath path) {
      if (path.getFirstNode().isAlias()) {
         String alias = path.getFirstNode().getName();
         if (aliasToEntityType.containsKey(alias)) {
            // Alias for entity
            return path.getNodeNamesWithoutAlias();
         } else if (aliasToPropertyPath.containsKey(alias)) {
            // Alias for embedded
            PropertyPath propertyPath = aliasToPropertyPath.get(alias);
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

   protected PathedPropertyReferenceSource normalizeProperty(FilterTypeDescriptor type, List<String> path, String propertyName) {
      if (!type.hasProperty(propertyName)) {
         throw log.getNoSuchPropertyException(type.toString(), propertyName);
      }

      FilterTypeDescriptor propType;
      if (type.hasEmbeddedProperty(propertyName)) {
         List<String> newPath = new LinkedList<>(path);
         newPath.add(propertyName);
         propType = new FilterEmbeddedEntityTypeDescriptor(type.getEntityType(), newPath, propertyHelper);
      } else {
         propType = new FilterPropertyTypeDescriptor();
      }
      return new PathedPropertyReference(propertyName, propType, false);
   }

   @Override
   public void pushFromStrategy(JoinType joinType, Tree associationFetchTree, Tree propertyFetchTree, Tree alias) {
      status = Status.DEFINING_FROM;
      this.alias = alias.getText();
   }

   @Override
   public void pushSelectStrategy() {
      status = Status.DEFINING_SELECT;
   }

   @Override
   public void popStrategy() {
      status = null;
      alias = null;
   }

   @Override
   public void propertyPathCompleted(PropertyPath path) {
      if (status == Status.DEFINING_SELECT && path.getLastNode().getType() instanceof FilterEmbeddedEntityTypeDescriptor) {
         FilterEmbeddedEntityTypeDescriptor type = (FilterEmbeddedEntityTypeDescriptor) path.getLastNode().getType();
         throw log.getProjectionOfCompleteEmbeddedEntitiesNotSupportedException(type.getEntityType(), path.asStringPathWithoutAlias());
      }
   }
}
