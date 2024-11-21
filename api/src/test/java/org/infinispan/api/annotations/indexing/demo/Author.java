package org.infinispan.api.annotations.indexing.demo;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.GeoPoint;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Latitude;
import org.infinispan.api.annotations.indexing.Longitude;

/**
 * Example of use of the new Infinispan indexing annotations.
 * <p>
 * Instance of indexed embedded entity.
 * The root indexed entity containing this entity is {@link Book}.
 *
 * @since 15.1
 */
@GeoPoint(fieldName = "placeOfBirth")
@GeoPoint(fieldName = "placeOfDeath")
public class Author {

   @Keyword(sortable = true)
   private String firstname;

   @Keyword
   private String surname;

   @Basic(name = "publications")
   @Basic(name = "books", searchable = false, sortable = true, projectable = true, aggregable = true)
   private Integer numberOfPublishedBooks;

   @Latitude(fieldName = "placeOfBirth")
   private Double placeOfBirthLatitude;

   @Longitude(fieldName = "placeOfBirth")
   private Double placeOfBirthLongitude;

   @Latitude(fieldName = "placeOfDeath")
   private Double placeOfDeathLatitude;

   @Longitude(fieldName = "placeOfDeath")
   private Double placeOfDeathLongitude;

}
