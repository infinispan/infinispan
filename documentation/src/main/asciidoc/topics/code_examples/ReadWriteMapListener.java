import org.infinispan.functional.EntryView.*;
import org.infinispan.functional.FunctionalMap.*;
import org.infinispan.functional.Listeners.ReadWriteListeners.ReadWriteListener;

ReadWriteMap<String, String> rwMap = ...
AutoCloseable readWriteClose = rwMap.listeners.add(new ReadWriteListener<String, String>() {
   @Override
   public void onCreate(ReadEntryView<String, String> created) {
      System.out.printf("Created: %s%n", created.get());
   }

   @Override
   public void onModify(ReadEntryView<String, String> before, ReadEntryView<String, String> after) {
      System.out.printf("Before: %s%n", before.get());
      System.out.printf("After: %s%n", after.get());
   }

   @Override
   public void onRemove(ReadEntryView<String, String> removed) {
      System.out.printf("Removed: %s%n", removed.get());
   }
);
AutoCloseable writeClose = rwMap.listeners.add(new WriteListener<String, String>() {
   @Override
   public void onWrite(ReadEntryView<K, V> written) {
      System.out.printf("Written: %s%n", written.get());
   }
);

// Either wrap handler in a try section to have it auto close...
try(readWriteClose) {
   // Create/update/remove entries using read-write functional map API
   ...
}
// Or close manually
writeClose.close();
