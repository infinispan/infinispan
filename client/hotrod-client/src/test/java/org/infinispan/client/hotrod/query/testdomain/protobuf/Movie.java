package org.infinispan.client.hotrod.query.testdomain.protobuf;

/**
 * @author Fabio Massimo Ercoli
 * @since 9.4
 */
public class Movie {

   public String id;
   public Integer genre;
   public Long releaseDate;
   public String suitableForKids;
   public String title;
   public byte rating;
   public Integer views;

   public Movie() {
   }

   public Movie(String id, Integer genre, Long releaseDate, String suitableForKids, String title, byte rating, Integer views) {
      this.id = id;
      this.genre = genre;
      this.releaseDate = releaseDate;
      this.suitableForKids = suitableForKids;
      this.title = title;
      this.rating = rating;
      this.views = views;
   }
}
