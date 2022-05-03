// create or obtain your RemoteCacheManager
RemoteCacheManager manager = ...;

// retrieve the MultimapCacheManager
MultimapCacheManager multimapCacheManager = RemoteMultimapCacheManagerFactory.from(manager);

// retrieve the RemoteMultimapCache
RemoteMultimapCache<Integer, String> people = multimapCacheManager.get("people");

// add key - values
people.put("coders", "Will");
people.put("coders", "Auri");
people.put("coders", "Pedro");

// retrieve single key with multiple values
Collection<String> coders = people.get("coders").join();