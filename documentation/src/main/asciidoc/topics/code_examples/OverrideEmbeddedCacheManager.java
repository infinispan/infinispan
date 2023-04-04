...
import jakarta.transaction.context.ApplicationScoped;

public class Config {

    @Produces
    @ApplicationScoped
    public EmbeddedCacheManager defaultEmbeddedCacheManager() {
      return new DefaultCacheManager(new ConfigurationBuilder()
                                          .memory()
                                              .size(100)
                                          .build());
   }
}
