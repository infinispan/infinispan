import org.infinispan.functional.EntryView.*;
import org.infinispan.functional.FunctionalMap.*;

ReadWriteMap<String, String> rwMap = ...
AutoCloseable createClose = rwMap.listeners().onCreate(created -> {
   // `created` is a ReadEntryView of the created entry
   System.out.printf("Created: %s%n", created.get());
});
AutoCloseable modifyClose = rwMap.listeners().onModify((before, after) -> {
   // `before` is a ReadEntryView of the entry before update
   // `after` is a ReadEntryView of the entry after update
   System.out.printf("Before: %s%n", before.get());
   System.out.printf("After: %s%n", after.get());
});
AutoCloseable removeClose = rwMap.listeners().onRemove(removed -> {
   // `removed` is a ReadEntryView of the removed entry
   System.out.printf("Removed: %s%n", removed.get());
});
AutoCloseable writeClose = woMap.listeners().onWrite(written -> {
   // `written` is a ReadEntryView of the written entry
   System.out.printf("Written: %s%n", written.get());
});
...
// Either wrap handler in a try section to have it auto close...
try(createClose) {
   // Create entries using read-write functional map API
   ...
}
// Or close manually
modifyClose.close();
