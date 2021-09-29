ConfigurationBuilder builder = new ConfigurationBuilder();
builder.expiration().lifespan(5000, TimeUnit.MILLISECONDS)
                    .maxIdle(1000, TimeUnit.MILLISECONDS);
