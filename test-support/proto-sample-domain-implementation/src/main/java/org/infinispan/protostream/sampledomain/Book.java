package org.infinispan.protostream.sampledomain;

import java.util.LinkedHashMap;
import java.util.Map;

import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;

@Indexed
public class Book {

   private final String title;
   private final String description;

    @ProtoFactory
    public Book(String title, String description) {
       this.title = title;
       this.description = description;
    }

    public static Map<Integer, Book> data() {
       Map<Integer, Book> data = new LinkedHashMap<>();
       data.put(1, new Book("1984", """
             Set in a dystopian, totalitarian future, the novel follows Winston Smith, a low-ranking party worker in a surveillance state ruled by Big Brother.
             Winston secretly rebels against the regime's intense brainwashing and history manipulation, entering into a
             forbidden romance while attempting to maintain his individuality and search for truth."""));
       data.put(2, new Book("To Kill a Mockingbird", """
             Set in 1930s Alabama, the novel is narrated by young Scout Finch, whose widowed father, lawyer Atticus Finch, is appointed to defend Tom Robinson—a Black man falsely accused of raping a white woman.
             As the trial exposes deep-seated racial prejudice in the town, Scout and her brother learn core lessons about empathy, integrity, and morality."""));
       data.put(3, new Book("The Great Gatsby", """
             Set in the roaring 1920s on Long Island, the novel is narrated by Nick Carraway, who gets drawn into the world of his enigmatic neighbor, Jay Gatsby.
             Gatsby throws extravagant parties in hopes of winning back his lost love, Daisy Buchanan, exposing the illusion, obsession, and moral decay underlying the American Dream."""));
       return data;
    }

   @ProtoField(value = 1)
   @Keyword(projectable = true, normalizer = "lowercase", sortable = true)
   public String getTitle() {
      return title;
   }

   @ProtoField(value = 2)
   @Text(analyzer = "whitespace", name = "naming")
   public String getDescription() {
       return description;
   }

   @ProtoSchema(
         includeClasses = Book.class,
         service = false
   )
   public interface BookSchema extends GeneratedSchema {
      BookSchema INSTANCE = new BookSchemaImpl();
   }
}
