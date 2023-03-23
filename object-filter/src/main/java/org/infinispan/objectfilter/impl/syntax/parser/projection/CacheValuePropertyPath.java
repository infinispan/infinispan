package org.infinispan.objectfilter.impl.syntax.parser.projection;

import java.util.Collections;

import org.infinispan.objectfilter.impl.ql.PropertyPath;

public class CacheValuePropertyPath<TypeDescriptor> extends PropertyPath<TypeDescriptor> {

   public static final String VALUE_PROPERTY_NAME = "__HSearch_This";

   public CacheValuePropertyPath() {
      super(Collections.singletonList(new PropertyPath.PropertyReference<>(VALUE_PROPERTY_NAME, null, true)));
   }
}
