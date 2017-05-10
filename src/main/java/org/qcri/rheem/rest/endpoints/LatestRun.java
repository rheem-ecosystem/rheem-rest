package org.qcri.rheem.rest.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.monitor.Monitor;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.rest.config.Config;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@Path("latest_run")
public class LatestRun {

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String get() throws IOException {
        String response;
        //response = "{\"run_id\":\"1\",\"stages\":[{\"sequence_number\":0,\"operators\":[{\"is_start\":1,\"is_terminal\":0,\"name\":\"mySource\",\"java_class\":\"org.qcri.rheem.java.operators.JavaTextFileSource\",\"connects_to\":{\"0\":[{\"flatMapOperator\":0,\"via\":\"StreamChannel\"}]}},{\"is_start\":0,\"is_terminal\":0,\"name\":\"flatMapOperator\",\"java_class\":\"org.qcri.rheem.java.operators.JavaFlatMapOperator\",\"connects_to\":{\"0\":[{\"mapOperator\":0,\"via\":\"StreamChannel\"}]}},{\"is_start\":0,\"is_terminal\":0,\"name\":\"mapOperator\",\"java_class\":\"org.qcri.rheem.java.operators.JavaMapOperator\",\"connects_to\":{\"0\":[{\"reduceOp\":0,\"via\":\"StreamChannel\"}]}},{\"is_start\":0,\"is_terminal\":0,\"name\":\"reduceOp\",\"java_class\":\"org.qcri.rheem.java.operators.JavaReduceByOperator\",\"connects_to\":{\"0\":[{\"sinkoperator1\":0,\"via\":\"CollectionChannel\"}]}},{\"is_start\":0,\"is_terminal\":1,\"name\":\"sinkoperator1\",\"java_class\":\"org.qcri.rheem.java.operators.JavaLocalCallbackSink\",\"connects_to\":{}}],\"platform\":\"Java Streams\"}]}";
        Configuration rheemConf = new Configuration(Config.rheemPropertiesUrl);

        String runsDir = rheemConf.getStringProperty(Monitor.DEFAULT_MONITOR_BASE_URL_PROPERTY_KEY,
                Monitor.DEFAULT_MONITOR_BASE_URL);

        String latest_run_id = "1";

        final String path = runsDir + "/" + latest_run_id;
        final String exPlanUrl = path + "/execplan.json";
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