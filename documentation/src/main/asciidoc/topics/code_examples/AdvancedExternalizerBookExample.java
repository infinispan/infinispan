import org.infinispan.marshall.AdvancedExternalizer;

public class Book {

   final String name;
   final String author;

   public Book(String name, String author) {
      this.name = name;
      this.author = author;
   }

   public static class BookExternalizer
            implements AdvancedExternalizer<Book> {

      @Override
      public void writeObject(ObjectOutput output, Book book)
            throws IOException {
         output.writeObject(book.name);
         output.writeObject(book.author);
      }

      @Override
      public Person readObject(ObjectInput input)
            throws IOException, ClassNotFoundException {
         return new Person((String) input.readObject(), (String) input.readObject());
      }

      @Override
      public Set<Class<? extends Book>> getTypeClasses() {
         return Util.<Class<? extends Book>>asSet(Book.class);
      }

      @Override
      public Integer getId() {
         return 2345;
      }
   }
}
