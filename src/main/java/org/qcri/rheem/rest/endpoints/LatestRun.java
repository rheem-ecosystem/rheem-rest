package org.qcri.rheem.rest.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.monitor.Monitor;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.rest.config.Config;
import org.qcri.rheem.rest.util.ExecPlanUtil;
import org.qcri.rheem.rest.util.JsonWrapper;

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
import java.util.List;
import java.util.Map;

@Path("latest_run")
public class LatestRun {

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String get() throws IOException {
        String response = "";
        //response = "{\"run_id\":\"1\",\"stages\":[{\"sequence_number\":0,\"operators\":[{\"is_start\":1,\"is_terminal\":0,\"name\":\"mySource\",\"java_class\":\"org.qcri.rheem.java.operators.JavaTextFileSource\",\"connects_to\":{\"0\":[{\"flatMapOperator\":0,\"via\":\"StreamChannel\"}]}},{\"is_start\":0,\"is_terminal\":0,\"name\":\"flatMapOperator\",\"java_class\":\"org.qcri.rheem.java.operators.JavaFlatMapOperator\",\"connects_to\":{\"0\":[{\"mapOperator\":0,\"via\":\"StreamChannel\"}]}},{\"is_start\":0,\"is_terminal\":0,\"name\":\"mapOperator\",\"java_class\":\"org.qcri.rheem.java.operators.JavaMapOperator\",\"connects_to\":{\"0\":[{\"reduceOp\":0,\"via\":\"StreamChannel\"}]}},{\"is_start\":0,\"is_terminal\":0,\"name\":\"reduceOp\",\"java_class\":\"org.qcri.rheem.java.operators.JavaReduceByOperator\",\"connects_to\":{\"0\":[{\"sinkoperator1\":0,\"via\":\"CollectionChannel\"}]}},{\"is_start\":0,\"is_terminal\":1,\"name\":\"sinkoperator1\",\"java_class\":\"org.qcri.rheem.java.operators.JavaLocalCallbackSink\",\"connects_to\":{}}],\"platform\":\"Java Streams\"}]}";
        Configuration rheemConf = new Configuration(Config.rheemPropertiesUrl);

        String runsDir = rheemConf.getStringProperty(Monitor.DEFAULT_MONITOR_BASE_URL_PROPERTY_KEY,
                Monitor.DEFAULT_MONITOR_BASE_URL);

        String latest_run_id = "1";

        final String path = runsDir + "/" + latest_run_id;
        final String exPlanUrl = path + "/execplan.json";
        try {
            if(Files.exists(Paths.get(new URI(exPlanUrl)))){
                byte[] encoded = Files.readAllBytes(Paths.get(new URI(exPlanUrl)));
                response = new String(encoded, Charset.defaultCharset());

                JSONParser parser = new JSONParser();
                JSONObject exeJson = (JSONObject)parser.parse(response);

                Map aMap = JsonWrapper.jsonToMap(exeJson);

                //System.out.println(aMap);

                List<Map> stages = (List)aMap.get("stages");

                boolean isUpdated = false;

                for(int i=0; i < stages.size(); i++){
                    Map stage = stages.get(i);
                    List<Map> ops = (List)stage.get("operators");
                    //System.out.println(ops);

                    if(!ExecPlanUtil.hasOnlyOneStartPoint(ops)){
                        List<Map> update_ops = ExecPlanUtil.sortStartingNodes(ops);
                        stages.get(i).put("operators", update_ops);
                        isUpdated = true;

                    }
                }

                if(isUpdated){
                    aMap.put("stages", stages);
                    exeJson = JsonWrapper.toJson(aMap);

                    response = exeJson.toJSONString();
                }

            }

        }catch (Exception e) {
            e.printStackTrace();
            response = "";
        }
        return response;
    }
}