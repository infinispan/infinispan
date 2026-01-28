package org.infinispan.query.objectfilter.impl.syntax.parser.projection;

import java.util.Collections;

import org.infinispan.query.objectfilter.impl.ql.PropertyPath;

public class VersionPropertyPath<TypeDescriptor> extends PropertyPath<TypeDescriptor> {

   public static final String VERSION_PROPERTY_NAME = "__ISPN_Version";

   public VersionPropertyPath() {
      super(Collections.singletonList(new PropertyReference<>(VERSION_PROPERTY_NAME, null, true)));
   }
}
