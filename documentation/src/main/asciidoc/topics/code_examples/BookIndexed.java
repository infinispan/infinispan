import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@ProtoDoc("@Indexed")
public class Book {

   @ProtoDoc("@Field(index=Index.YES, analyze = Analyze.YES, store = Store.NO)")
   @ProtoField(number = 1)
   final String title;

   @ProtoDoc("@Field(index=Index.YES, analyze = Analyze.YES, store = Store.NO)")
   @ProtoField(number = 2)
   final String description;

   @ProtoDoc("@Field(index=Index.YES, analyze = Analyze.YES, store = Store.NO)")
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