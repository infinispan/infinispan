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
package org.infinispan.objectfilter.impl.ql.parse;

import org.antlr.runtime.CommonToken;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;

/**
 * A {@link CommonTree} representing an entity name.
 *
 * @since 9.0
 */
final class EntityNameTree extends CommonTree {

   private final String entityName;

   public EntityNameTree(int tokenType, Token token, String tokenText, Tree entityNameTree) {
      super(token);
      Token newToken = new CommonToken(token);
      newToken.setType(tokenType);
      newToken.setText(tokenText);
      this.token = newToken;
      this.entityName = toString(entityNameTree);
   }

   private static String toString(Tree tree) {
      switch (tree.getChildCount()) {
         case 0:
            // a single argument
            return tree.getText();
         case 1:
            // an unary operator and the argument
            return tree.getText() + toString(tree.getChild(0));
         case 2:
            // a binary operator and its arguments
            return toString(tree.getChild(0)) + tree.getText() + toString(tree.getChild(1));
         default:
            throw new IllegalStateException("Only unary or binary operators expected.");
      }
   }

   public String getEntityName() {
      return entityName;
   }

   @Override
   public String toString() {
      return entityName;
   }
}
