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

/**
 * Represents a canonical join type.
 * <p>
 * Note that currently HQL really only supports inner and left outer joins
 * (though cross joins can also be achieved).  This is because joins in HQL
 * are always defined in relation to a mapped association.  However, when we
 * start allowing users to specify ad-hoc joins this may need to change to
 * allow the full spectrum of join types.  Thus the others are provided here
 * currently just for completeness and for future expansion.
 *
 * @author Steve Ebersole
 * @since 9.0
 */
public enum JoinType {

   /**
    * Represents an inner join.
    */
   INNER("inner"),

   /**
    * Represents a left outer join.
    */
   LEFT("left outer"),

   /**
    * Represents a right outer join.
    */
   RIGHT("right outer"),

   /**
    * Represents a cross join (aka a cartesian product).
    */
   CROSS("cross"),

   /**
    * Represents a full join.
    */
   FULL("full");

   private final String name;

   JoinType(String name) {
      this.name = name;
   }

   @Override
   public String toString() {
      return name;
   }
}
