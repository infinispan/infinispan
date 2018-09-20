package org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers;

import java.io.IOException;

import org.infinispan.client.hotrod.query.testdomain.protobuf.Movie;
import org.infinispan.protostream.MessageMarshaller;

/**
 * @author Fabio Massimo Ercoli
 * @since 9.4
 */
public class MovieMarshaller implements MessageMarshaller<Movie> {

   public Movie readFrom(ProtoStreamReader reader) throws IOException {
      String id = reader.readString("id");
      Integer genre = reader.readInt("genre");
      Long releaseDate = reader.readLong("releaseDate");
      String suitableForKids = reader.readString("suitableForKids");
      String title = reader.readString("title");
      byte[] viewerRatings = reader.readBytes("rating");
      Integer views = reader.readInt("views");

      return new Movie(id, genre, releaseDate, suitableForKids, title, MovieMarshaller.toSingle(viewerRatings), views);
   }

   public void writeTo(ProtoStreamWriter writer, Movie movie) throws IOException {
      writer.writeString("id", movie.id);
      writer.writeInt("genre", movie.genre);
      writer.writeLong("releaseDate", movie.releaseDate);
      writer.writeString("suitableForKids", movie.suitableForKids);
      writer.writeString("title", movie.title);
      writer.writeBytes("rating", MovieMarshaller.toArray(movie.rating));
      writer.writeInt("views", movie.views);
   }

   public Class<? extends Movie> getJavaClass() {
      return Movie.class;
   }

   public String getTypeName() {
      return "sample_bank_account.Movie";
   }

   public static byte[] toArray(byte single) {
      byte[] array = new byte[1];
      array[0] = single;
      return array;
   }

   public static byte toSingle(byte[] array) {
      return array[0];
   }
}
