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
import org.infinispan.api.annotations.indexing.option.Aggregable;
import org.infinispan.api.annotations.indexing.option.Norms;
import org.infinispan.api.annotations.indexing.option.Projectable;
import org.infinispan.api.annotations.indexing.option.Searchable;
import org.infinispan.api.annotations.indexing.option.Sortable;
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

   @Keyword(normalizer = "lowercase", projectable = Projectable.YES)
   private String keyword;

   @Basic(name = "year", searchable = Searchable.YES)
   private Integer yearOfPublication;

   @Text(name = "descriptionText", analyzer = "english", termVector = TermVector.YES, norms = Norms.YES)
   @Basic(name = "description", projectable = Projectable.YES, sortable = Sortable.YES)
   private String description;

   @Decimal(decimalScale = 2, aggregable = Aggregable.YES)
   private Float price;

   @Embedded(structure = Structure.FLATTENED)
   private Author author;

   @Embedded(structure = Structure.NESTED)
   private List<Review> reviews;

   @GeoCoordinates
   private Point placeOfPublication;

}
