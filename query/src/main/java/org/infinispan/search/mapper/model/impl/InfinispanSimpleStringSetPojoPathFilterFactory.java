package org.infinispan.search.mapper.model.impl;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.hibernate.search.mapper.pojo.model.path.PojoModelPathPropertyNode;
import org.hibernate.search.mapper.pojo.model.path.PojoModelPathValueNode;
import org.hibernate.search.mapper.pojo.model.path.spi.PojoPathsDefinition;

/**
 * A factory for filters expecting a simple string representation of dirty paths, in the form
 * "propertyA.propertyB.propertyC".
 * <p>
 * See {@link PojoModelPathPropertyNode#toPropertyString()}.
 */
public class InfinispanSimpleStringSetPojoPathFilterFactory implements PojoPathsDefinition {

   @Override
   public List<String> preDefinedOrdinals() {
      return Collections.emptyList(); // No pre-defined ordinals
   }

   @Override
   public void interpretPaths(Set<String> target, Set<PojoModelPathValueNode> source) {
      for (PojoModelPathValueNode path : source) {
         target.add(path.parent().toPropertyString());
      }
   }
}
