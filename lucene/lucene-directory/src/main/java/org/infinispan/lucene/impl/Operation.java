package org.infinispan.lucene.impl;

import java.util.Set;

/**
 * @author gustavonalle
 * @since 7.0
 */
interface Operation {

   void apply(Set<String> target);

}
