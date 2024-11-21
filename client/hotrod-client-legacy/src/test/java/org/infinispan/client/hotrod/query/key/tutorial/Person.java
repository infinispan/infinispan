package org.infinispan.client.hotrod.query.key.tutorial;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.protostream.annotations.Proto;

@Proto
@Indexed(keyEntity="tutorial.PersonKey", keyPropertyName = "mykey")
public record Person(
      @Keyword(projectable = true, sortable = true, normalizer = "lowercase", indexNullAs = "unnamed", norms = false)
      String firstName,
      @Keyword(projectable = true, sortable = true, normalizer = "lowercase", indexNullAs = "unnamed", norms = false)
      String lastName,
      @Basic
      int bornYear,
      @Keyword(projectable = true, sortable = true, normalizer = "lowercase", indexNullAs = "unnamed", norms = false)
      String bornIn,
      @Basic(projectable = true)
      int key
) {
}
