package org.infinispan.api.annotations.indexing.demo;

import java.util.List;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Decimal;
import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.GeoCoordinates;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.api.annotations.indexing.model.Point;
import org.infinispan.api.annotations.indexing.option.Structure;
import org.infinispan.api.annotations.indexing.option.TermVector;

/**
 * Example of use of the new Infinispan indexing annotations.
 * <p>
 * Instance of indexed (as root) entity,
 * containing two embedded indexed entities: {@link Author} and {@link Review}.
 *
 * @since 14.0
 */
@Indexed(index = "books")
public class Book {

   @Keyword(normalizer = "lowercase", projectable = true)
   private String keyword;

   @Basic(name = "year", searchable = true)
   private Integer yearOfPublication;

   @Text(name = "descriptionText", analyzer = "english", termVector = TermVector.WITH_POSITIONS_OFFSETS_PAYLOADS, norms = false)
   @Basic(name = "description", projectable = true, sortable = true)
   private String description;

   @Decimal(decimalScale = 2, aggregable = true)
   private Float price;

   @Embedded(structure = Structure.FLATTENED)
   private Author author;

   @Embedded(structure = Structure.NESTED)
   private List<Review> reviews;

   @GeoCoordinates
   private Point placeOfPublication;

}
