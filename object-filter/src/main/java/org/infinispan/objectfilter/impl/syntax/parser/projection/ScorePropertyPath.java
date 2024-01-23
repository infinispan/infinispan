package org.infinispan.objectfilter.impl.syntax.parser.projection;

import java.util.Collections;

import org.infinispan.objectfilter.impl.ql.PropertyPath;

public class ScorePropertyPath<TypeDescriptor> extends PropertyPath<TypeDescriptor> {

   public static final String SCORE_PROPERTY_NAME = "__ISPN_Score";

   public ScorePropertyPath() {
      super(Collections.singletonList(new PropertyReference<>(SCORE_PROPERTY_NAME, null, true)));
   }
}
