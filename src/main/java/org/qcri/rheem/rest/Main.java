package org.qcri.rheem.rest;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.rest.config.CORSFilter;
import org.qcri.rheem.rest.config.Config;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main class.
 *
 */
public class Main {
    public static final String BASE_URI = Config.BASE_URI;


    /**
     * Starts Grizzly HTTP server exposing JAX-RS resources defined in this application.
     * @return Grizzly HTTP server.
     */
    public static HttpServer startServer() {
        // create a resource config that scans for JAX-RS resources and providers
        // in org.qcri.rheem package
        final ResourceConfig rc = new ResourceConfig().packages("org.qcri.rheem.rest");
        //rc.register(new CORSFilter());
        // create and start a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI
        return GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
    }

    /**
     * Main method.
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        Logger log = Logger.getLogger(Main.class.getName());

        log.log(Level.WARNING,"Starting server .....");

        Logger log2 = Logger.getLogger("org.glassfish.grizzly");
        log2.setLevel(Level.ALL);
        log2.addHandler(new java.util.logging.ConsoleHandler());

        final HttpServer server = startServer();

        System.out.println(String.format("Jersey app started with WADL available at "
                + "%sapplication.wadl\nHit enter to stop it...", BASE_URI));
        System.in.read();
        server.stop();
    }
}

