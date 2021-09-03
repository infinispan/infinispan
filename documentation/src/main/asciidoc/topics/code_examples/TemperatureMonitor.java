@ClientListener
public class TemperatureChangesListener {
   private String location;

   TemperatureChangesListener(String location) {
      this.location = location;
   }

   @ClientCacheEntryCreated
   public void created(ClientCacheEntryCreatedEvent event) {
      if(event.getKey().equals(location)) {
         cache.getAsync(location)
               .whenComplete((temperature, ex) ->
                     System.out.printf(">> Location %s Temperature %s", location, temperature));
      }
   }
}
