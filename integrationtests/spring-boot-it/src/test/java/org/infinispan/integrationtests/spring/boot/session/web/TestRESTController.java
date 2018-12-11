package org.infinispan.integrationtests.spring.boot.session.web;

import javax.servlet.http.HttpServletRequest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class TestRESTController {

   @RequestMapping("/test")
   public ResponseEntity testRest(HttpServletRequest request){
      return new ResponseEntity(HttpStatus.OK);
   }

}
