package org.qcri.rheem.rest.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.rest.config.Config;
import org.qcri.rheem.rest.exception.RheemRestException;
import org.qcri.rheem.rest.model.Core;
import org.qcri.rheem.spark.Spark;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *  Resource (exposed at "plans" path)
 */
@Path("rheem_plans")
public class Plan {
    private static Logger logger = Logger.getLogger(Plan.class);
    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "text/plain" media type.
     *
     * @return String that will be returned as a text/plain response.
     */
    @GET @Path("{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Map get(@PathParam("id") String id) throws IOException {
        String response = "{\"registered_platforms\": [{\"name\": \"Apache Spark\"}, {\"name\": \"Java Streams\"}], \"sink_operators\": [\"sinkoperator1\"], \"operators\": [{\"connects_to\": {\"0\": [{\"flatMapOperator\": 0}]}, \"name\": \"source1\", \"parameters\": {\"inputUrl\": \"some-lines.txt\"}, \"java_class\": \"TextFileSource\"}, {\"connects_to\": {\"0\": [{\"mapOperator\": 0}]}, \"name\": \"flatMapOperator\", \"parameters\": {\"function\": \"line -> Arrays.asList(line.split())\", \"inputTypeClass\": \"String\", \"outputTypeClass\": \"String\"}, \"java_class\": \"FlatMapOperator\"}, {\"connects_to\": {\"0\": [{\"reduceByOperator\": 0}]}, \"name\": \"mapOperator\", \"parameters\": {\"function\": \"word -> new Tuple2<>(word.toLowerCase(), 1)\", \"inputTypeClass\": \"String\", \"outputTypeClass\": \"Tuple2\"}, \"java_class\": \"MapOperator\"}, {\"connects_to\": {\"0\": [{\"sinkoperator1\": 0}]}, \"name\": \"reduceByOperator\", \"parameters\": {\"reduceDescriptor\": \"(a, b) -> {((Tuple2<String, Integer>)a).field1 += ((Tuple2<String, Integer>)b).field1;return a;}\", \"keyClass\": \"String\", \"keyFunction\": \"pair -> ((Tuple2<String, Integer>)pair).field0\", \"typeClass\": \"Tuple2\"}, \"java_class\": \"ReduceByOperator\"}, {\"connects_to\": {\"0\": [{\"flatMapOperator\": 0}]}, \"name\": \"sinkoperator1\", \"parameters\": {\"typeClass\": \"Tuple2\"}, \"java_class\": \"LocalCallbackSink\"}], \"name\": \"plan1\", \"id\": 1}";
        logger.info(response);
        HashMap<String,Object> myMap = new ObjectMapper().readValue(response, HashMap.class);
        return myMap;
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Map create(Map inputJsonObj) throws IOException {

        Map<String, Object> response = new HashMap<>();

        try {
            System.out.println(inputJsonObj);

           // RheemContext rheemContext = new RheemContext(new Configuration(Config.rheemPropertiesUrl));
            RheemContext rheemContext = new RheemContext();
            rheemContext.register(Java.basicPlugin());

            //rheemContext.register(Spark.basicPlugin());
            RheemPlan rheemPlan = Core.getRheemPlanFromJson(inputJsonObj);
            ExecutionPlan execplan = rheemContext.buildInitialExecutionPlan("1", rheemPlan);
            response.put("stages", execplan.toJsonList());
        } catch (RheemRestException ee) {
            logger.error(ee);
            System.out.println("Runtime error" + ee);
        } catch (Exception e) {
            logger.error(e);
            System.out.println("Error: " + e);
            e.printStackTrace();
            response.put("error", e.toString());
        }

        return response;
    }
}
