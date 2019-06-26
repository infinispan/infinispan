@Listener (sync = false)
public class MyAsyncListener {
   @CacheEntryCreated
   void listen(CacheEntryCreatedEvent event) { }
}
