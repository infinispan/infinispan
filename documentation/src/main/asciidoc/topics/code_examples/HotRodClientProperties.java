ConfigurationBuilder b = new ConfigurationBuilder();
Properties p = new Properties();
try(Reader r = new FileReader("/path/to/hotrod-client.properties")) {
   p.load(r);
   b.withProperties(p);
}
RemoteCacheManager rcm = new RemoteCacheManager(b.build());
