@Listener
public class MyNonBlockingListener {
   @CacheEntryCreated
   CompletionStage<Void> listen(CacheEntryCreatedEvent event) { }
}
