import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class Book {

   @Text
   @ProtoField(number = 1)
   final String title;

   @Text
   @ProtoField(number = 2)
   final String description;

   @Basic
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