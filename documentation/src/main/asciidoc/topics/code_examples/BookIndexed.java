import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Index;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class Book {

   @Field(index=Index.YES, analyze = Analyze.YES, store = Store.NO)
   @ProtoField(number = 1)
   final String title;

   @Field(index=Index.YES, analyze = Analyze.YES, store = Store.NO)
   @ProtoField(number = 2)
   final String description;

   @Field(index=Index.YES, store = Store.NO)
   @ProtoField(number = 3, defaultValue = "0")
   final int publicationYear;


   @ProtoFactory
   Book(String title, String description, int publicationYear) {
      this.title = title;
      this.description = description;
      this.publicationYear = publicationYear;
   }
   // public Getter methods omitted for brevity
}