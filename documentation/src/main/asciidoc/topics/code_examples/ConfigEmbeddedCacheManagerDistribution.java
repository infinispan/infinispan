private static EmbeddedCacheManager createCacheManagerFromXml() throws IOException {
   return new DefaultCacheManager("infinispan-distribution.xml");
}
