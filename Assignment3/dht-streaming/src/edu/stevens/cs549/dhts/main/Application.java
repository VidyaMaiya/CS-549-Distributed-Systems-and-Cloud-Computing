package edu.stevens.cs549.dhts.main;

import org.glassfish.jersey.media.sse.SseFeature;
import org.glassfish.jersey.server.ResourceConfig;

import edu.stevens.cs549.dhts.resource.NodeResource;

public class Application extends ResourceConfig {
    public Application() {
        packages("edu.stevens.cs549.dhts.resource");
        // TODO register SseFeature
        register(SseFeature.class);
        register(NodeResource.class);
    }
}
