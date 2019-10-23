import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
...

public class Book {
   @ProtoField(number = 1)
   final String title;

   @ProtoField(number = 2)
   final String description;

   @ProtoField(number = 3, defaultValue = "0")
   final int publicationYear;

   @ProtoField(number = 4, collectionImplementation = ArrayList.class)
   final List<Author> authors;

   @ProtoFactory
   Book(String title, String description, int publicationYear, List<Author> authors) {
      this.title = title;
      this.description = description;
      this.publicationYear = publicationYear;
      this.authors = authors;
   }
   // public Getter methods omitted for brevity
}