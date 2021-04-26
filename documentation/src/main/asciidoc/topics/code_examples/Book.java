import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class Book {
   @ProtoField(number = 1)
   public final UUID id;

   @ProtoField(number = 2)
   final String title;

   @ProtoField(number = 3)
   final String description;

   @ProtoField(number = 4, defaultValue = "0")
   final int publicationYear;

   @ProtoField(number = 5, collectionImplementation = ArrayList.class)
   final List<Author> authors;

   @ProtoField(number = 6)
   public Language language;

   @ProtoFactory
   Book(UUID id, String title, String description, int publicationYear, List<Author> authors, Language language) {
      this.id = id;
      this.title = title;
      this.description = description;
      this.publicationYear = publicationYear;
      this.authors = authors;
      this.language = language;
   }
   // public Getter methods not included for brevity
}
