public class Config {

    // By default CDI adds the @Default qualifier if no other qualifier is provided.
    @Produces
    public Configuration defaultEmbeddedCacheConfiguration() {
        return new ConfigurationBuilder()
                    .memory()
                        .size(100)
                    .build();
    }
}
