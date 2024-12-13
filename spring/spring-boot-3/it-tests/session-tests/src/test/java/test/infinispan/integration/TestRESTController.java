package test.infinispan.integration;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestRESTController {

   @GetMapping("/test")
   public String testRest(){
      return "Ok";
   }
}
