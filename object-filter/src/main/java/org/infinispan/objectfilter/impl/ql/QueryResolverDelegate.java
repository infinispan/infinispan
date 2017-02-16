/*
 * Copyright 2016, Red Hat Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.infinispan.objectfilter.impl.ql;

import org.antlr.runtime.tree.Tree;

/**
 * Defines hooks for implementing custom logic when walking the parse tree of a JPQL query.
 *
 * @author Gunnar Morling
 * @author anistor@redhat.com
 * @since 9.0
 */
public interface QueryResolverDelegate<TypeDescriptor> {

   void registerPersisterSpace(String entityName, Tree alias);

   void registerJoinAlias(Tree alias, PropertyPath<TypeDescriptor> path);

   boolean isUnqualifiedPropertyReference();

   PropertyPath.PropertyReference<TypeDescriptor> normalizeUnqualifiedPropertyReference(Tree propertyNameTree);

   boolean isPersisterReferenceAlias();

   PropertyPath.PropertyReference<TypeDescriptor> normalizeUnqualifiedRoot(Tree aliasTree);

   PropertyPath.PropertyReference<TypeDescriptor> normalizeQualifiedRoot(Tree root);

   PropertyPath.PropertyReference<TypeDescriptor> normalizePropertyPathIntermediary(PropertyPath<TypeDescriptor> path, Tree propertyNameTree);

   PropertyPath.PropertyReference<TypeDescriptor> normalizeIntermediateIndexOperation(PropertyPath.PropertyReference<TypeDescriptor> propertyReference, Tree collectionProperty, Tree selector);

   void normalizeTerminalIndexOperation(PropertyPath.PropertyReference<TypeDescriptor> propertyReference, Tree collectionProperty, Tree selector);

   PropertyPath.PropertyReference<TypeDescriptor> normalizeUnqualifiedPropertyReferenceSource(Tree identifier);

   PropertyPath.PropertyReference<TypeDescriptor> normalizePropertyPathTerminus(PropertyPath<TypeDescriptor> path, Tree propertyNameTree);

   void activateFromStrategy(JoinType joinType, Tree associationFetchTree, Tree propertyFetchTree, Tree alias);

   void activateSelectStrategy();

   void deactivateStrategy();

   /**
    * Notifies this delegate when parsing of a property path in the SELECT or WHERE is completed.
    *
    * @param path the completely parsed property path
    */
   void propertyPathCompleted(PropertyPath<TypeDescriptor> path);
}
