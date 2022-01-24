AdvancedCache ac = cache.getAdvancedCache();
// Retrieve cache availability
boolean available = ac.getAvailability() == AvailabilityMode.AVAILABLE;
// Make the cache available
if (!available) {
   ac.setAvailability(AvailabilityMode.AVAILABLE);
}
