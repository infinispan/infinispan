package org.infinispan.api.annotations.indexing.demo;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.GeoCoordinates;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Latitude;
import org.infinispan.api.annotations.indexing.Longitude;

/**
 * Example of use of the new Infinispan indexing annotations.
 * <p>
 * Instance of indexed embedded entity.
 * The root indexed entity containing this entity is {@link Book}.
 *
 * @since 14.0
 */
@GeoCoordinates(fieldName = "placeOfBirth", marker = "birth")
@GeoCoordinates(fieldName = "placeOfDeath", marker = "death")
public class Author {

   @Keyword(sortable = true)
   private String firstname;

   @Keyword
   private String surname;

   @Basic(name = "publications")
   @Basic(name = "books", searchable = false, sortable = true, projectable = true, aggregable = true)
   private Integer numberOfPublishedBooks;

   @Latitude(marker = "birth")
   private Double placeOfBirthLatitude;

   @Longitude(marker = "birth")
   private Double placeOfBirthLongitude;

   @Latitude(marker = "death")
   private Double placeOfDeathLatitude;

   @Longitude(marker = "death")
   private Double placeOfDeathLongitude;

}
