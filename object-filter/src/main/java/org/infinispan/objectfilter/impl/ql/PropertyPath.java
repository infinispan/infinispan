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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;


/**
 * A property path (e.g. {@code foo.bar.baz}) represented by a {@link List} of {@link PropertyReference}s, used
 * in a SELECT, GROUP BY, ORDER BY, WHERE or HAVING clause.
 *
 * @author Gunnar Morling
 * @author anistor@redhat.com
 * @since 9.0
 */
public class PropertyPath<TypeDescriptor> {

   public static final class PropertyReference<TypeDescriptor> {

      private final String propertyName;

      private final TypeDescriptor typeDescriptor;

      private final boolean isAlias;

      public PropertyReference(String propertyName, TypeDescriptor typeDescriptor, boolean isAlias) {
         this.propertyName = propertyName;
         this.typeDescriptor = typeDescriptor;
         this.isAlias = isAlias;
      }

      public String getPropertyName() {
         return propertyName;
      }

      public boolean isAlias() {
         return isAlias;
      }

      public TypeDescriptor getTypeDescriptor() {
         return typeDescriptor;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || o.getClass() != getClass()) return false;

         PropertyReference<?> that = (PropertyReference<?>) o;
         return isAlias == that.isAlias && propertyName.equals(that.propertyName)
               && (typeDescriptor != null ? typeDescriptor.equals(that.typeDescriptor) : that.typeDescriptor == null);
      }

      @Override
      public int hashCode() {
         int result = propertyName.hashCode();
         result = 31 * result + (isAlias ? 1 : 0);
         result = 31 * result + (typeDescriptor != null ? typeDescriptor.hashCode() : 0);
         return result;
      }

      @Override
      public String toString() {
         return propertyName;
      }
   }

   private final LinkedList<PropertyReference<TypeDescriptor>> nodes;

   private List<PropertyReference<TypeDescriptor>> unmodifiableNodes;
   private String asStringPath;
   private String asStringPathWithoutAlias;
   private String[] asArrayPath;

   /**
    * Creates an empty path.
    */
   public PropertyPath() {
      this.nodes = new LinkedList<>();
   }

   /**
    * Creates an path with a given list of nodes.
    */
   public PropertyPath(List<PropertyReference<TypeDescriptor>> nodes) {
      this.nodes = new LinkedList<>(nodes);
   }

   public boolean isAlias() {
      return getFirst().isAlias();
   }

   public PropertyReference<TypeDescriptor> getFirst() {
      return nodes.getFirst();
   }

   public PropertyReference<TypeDescriptor> getLast() {
      return nodes.getLast();
   }

   public List<PropertyReference<TypeDescriptor>> getNodes() {
      if (unmodifiableNodes == null) {
         unmodifiableNodes = Collections.unmodifiableList(nodes);
      }
      return unmodifiableNodes;
   }

   public void append(PropertyReference<TypeDescriptor> propertyReference) {
      nodes.add(propertyReference);
      asArrayPath = null;
      asStringPath = null;
      asStringPathWithoutAlias = null;
   }

   public boolean isEmpty() {
      return nodes.isEmpty();
   }

   public int getLength() {
      return nodes.size();
   }

   public String asStringPath() {
      if (asStringPath == null) {
         StringBuilder sb = new StringBuilder();
         boolean isFirst = true;
         for (PropertyReference node : nodes) {
            if (isFirst) {
               isFirst = false;
            } else {
               sb.append('.');
            }

            sb.append(node.getPropertyName());
         }
         asStringPath = sb.toString();
      }
      return asStringPath;
   }

   public String asStringPathWithoutAlias() {
      if (asStringPathWithoutAlias == null) {
         StringBuilder sb = new StringBuilder();
         boolean isFirst = true;
         for (PropertyReference node : nodes) {
            if (!node.isAlias()) {
               if (isFirst) {
                  isFirst = false;
               } else {
                  sb.append('.');
               }

               sb.append(node.getPropertyName());
            }
         }
         asStringPathWithoutAlias = sb.toString();
      }
      return asStringPathWithoutAlias;
   }

   public String[] asArrayPath() {
      if (asArrayPath == null) {
         String[] arrayPath = new String[nodes.size()];
         int i = 0;
         for (PropertyReference<?> pr : nodes) {
            arrayPath[i++] = pr.getPropertyName();
         }
         asArrayPath = arrayPath;
      }
      return asArrayPath;
   }

   public List<String> getNodeNamesWithoutAlias() {
      List<String> list = new ArrayList<>(nodes.size());
      for (PropertyReference<TypeDescriptor> node : nodes) {
         if (!node.isAlias()) {
            list.add(node.getPropertyName());
         }
      }
      return list;
   }

   public List<PropertyReference<TypeDescriptor>> getNodesWithoutAlias() {
      List<PropertyReference<TypeDescriptor>> list = new ArrayList<>(nodes.size());
      for (PropertyReference<TypeDescriptor> node : nodes) {
         if (!node.isAlias()) {
            list.add(node);
         }
      }
      return list;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || o.getClass() != getClass()) return false;
      PropertyPath<?> that = (PropertyPath<?>) o;
      return nodes.equals(that.nodes);
   }

   @Override
   public int hashCode() {
      return nodes.hashCode();
   }

   @Override
   public String toString() {
      return asStringPath();
   }

   public static <TypeDescriptor> PropertyPath<TypeDescriptor> make(String propertyPath) {
      String[] splinters = propertyPath.split("[.]");
      List<PropertyReference<TypeDescriptor>> nodes = new ArrayList<>(splinters.length);
      for (String name : splinters) {
         nodes.add(new PropertyPath.PropertyReference<>(name, null, false));
      }
      return new PropertyPath<>(nodes);
   }
}
