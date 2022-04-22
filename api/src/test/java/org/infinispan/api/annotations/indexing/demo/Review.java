package org.infinispan.api.annotations.indexing.demo;

import java.time.LocalDate;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Decimal;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.api.annotations.indexing.option.Aggregable;
import org.infinispan.api.annotations.indexing.option.Projectable;
import org.infinispan.api.annotations.indexing.option.Sortable;

/**
 * Example of use of the new Infinispan indexing annotations.
 * <p>
 * Instance of indexed embedded entity.
 * The root indexed entity containing this entity is {@link Book}.
 *
 * @since 14.0
 */
public class Review {

   @Basic(sortable = Sortable.YES)
   private LocalDate date;

   @Text
   private String content;

   @Decimal(decimalScale = 1, sortable = Sortable.YES, projectable = Projectable.YES, aggregable = Aggregable.YES)
   private Float score;

}
