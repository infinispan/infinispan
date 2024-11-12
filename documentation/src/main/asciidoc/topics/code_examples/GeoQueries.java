class GeoQueries() {

   public void restaurants_withinCircle() {
      //tag::restaurants_withinCircle[]
      Query<Restaurant> query = cache.query("from geo.Restaurant r where r.location within circle(41.91, 12.46, :distance)"); // <1>
      query.setParameter("distance", 100); // <2>
      List<Restaurant> list = query.list();
      //end::restaurants_withinCircle[]
   }

   public void restaurants_withinBox() {
      //tag::restaurants_withinBox[]
      Query<Restaurant> query = cache.query("from geo.Restaurant r where r.location within box(41.91, 12.45, 41.90, 12.46)"); // <1>
      List<Restaurant> list = query.list();
      //end::restaurants_withinBox[]
   }

   public void restaurants_withinPolygon() {
      //tag::restaurants_withinPolygon[]
      Query<Restaurant> query = cache.query("from geo.Restaurant r where r.location within polygon((41.91, 12.45), (41.91, 12.46), (41.90, 12.46), (41.90, 12.46))"); // <1>
      List<Restaurant> list = query.list();
      //end::restaurants_withinPolygon[]
   }

   public void restaurants_orderByDistances() {
      //tag::restaurants_orderByDistances[]
      Query<Restaurant> query = cache.query("from geo.Restaurant r order by distance(r.location, 41.91, 12.46)"); // <1>
      List<Restaurant> list = query.list();
      //end::restaurants_orderByDistances[]
   }

   public void restaurants_projectDistances() {
      //tag::restaurants_projectDistances[]
      Query<Object[]> projectQuery = remoteCache.query("select r.name, distance(r.location, 41.91, 12.46) from %s geo.Restaurant r");
      List<Object[]> projectList = projectQuery.list();
      //end::restaurants_projectDistances[]
   }
}