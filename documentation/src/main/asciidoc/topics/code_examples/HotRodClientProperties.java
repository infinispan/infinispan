ConfigurationBuilder builder = new ConfigurationBuilder();
Properties p = new Properties();
try(Reader r = new FileReader("/path/to/hotrod-client.properties")) {
   p.load(r);
   builder.withProperties(p);
}
RemoteCacheManager cacheManager = new RemoteCacheManager(builder.build());
