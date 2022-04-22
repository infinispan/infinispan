package org.infinispan.api.annotations.indexing.demo;

import java.time.LocalDate;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Decimal;
import org.infinispan.api.annotations.indexing.Text;

/**
 * Example of use of the new Infinispan indexing annotations.
 * <p>
 * Instance of indexed embedded entity.
 * The root indexed entity containing this entity is {@link Book}.
 *
 * @since 14.0
 */
public class Review {

   @Basic(sortable = true)
   private LocalDate date;

   @Text
   private String content;

   @Decimal(decimalScale = 1, sortable = true, projectable = true, aggregable = true)
   private Float score;

}
