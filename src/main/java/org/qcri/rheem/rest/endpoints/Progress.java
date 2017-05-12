package org.qcri.rheem.rest.endpoints;


import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.monitor.Monitor;
import org.qcri.rheem.rest.config.Config;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

@Path("progress")
public class Progress {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String get() throws IOException {
        //String response = "{\"overall\":80, \"details\": {\"mySource\": 100}}";

        String response;
        Configuration rheemConf = new Configuration(Config.rheemPropertiesUrl);

        String runsDir = rheemConf.getStringProperty(Monitor.DEFAULT_MONITOR_BASE_URL_PROPERTY_KEY,
                Monitor.DEFAULT_MONITOR_BASE_URL);

        String latest_run_id = "1";

        final String path = runsDir + "/" + latest_run_id;
        final String exPlanUrl = path + "/progress.json";
        try {
            byte[] encoded = Files.readAllBytes(Paths.get(new URI(exPlanUrl)));
            response = new String(encoded, Charset.defaultCharset());

        }catch (Exception e) {
            e.printStackTrace();
            response = "";
        }
        return response;
    }
}


