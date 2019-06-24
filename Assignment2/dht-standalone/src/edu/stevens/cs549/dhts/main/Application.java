package edu.stevens.cs549.dhts.main;

import org.glassfish.jersey.server.ResourceConfig;

public class Application extends ResourceConfig {
    public Application() {
    	//Specifying packages where the web server should look for classes that define resources
        packages("edu.stevens.cs549.dhts.resource"); 
      //Grizzly look for REST implementation in there, will look for resource classes in there
    }
}
