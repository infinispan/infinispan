import org.infinispan.functional.EntryView.*;
import org.infinispan.functional.FunctionalMap.*;
import org.infinispan.functional.Listeners.WriteListeners.WriteListener;

WriteOnlyMap<String, String> woMap = ...

AutoCloseable writeFunctionCloseHandler = woMap.listeners().onWrite(written -> {
   // `written` is a ReadEntryView of the written entry
   System.out.printf("Written: %s%n", written.get());
});
AutoCloseable writeCloseHanlder = woMap.listeners().add(new WriteListener<String, String>() {
   @Override
   public void onWrite(ReadEntryView<K, V> written) {
      System.out.printf("Written: %s%n", written.get());
   }
});

// Either wrap handler in a try section to have it auto close...
try(writeFunctionCloseHandler) {
   // Write entries using read-write or write-only functional map API
   ...
}
// Or close manually
writeCloseHanlder.close();
