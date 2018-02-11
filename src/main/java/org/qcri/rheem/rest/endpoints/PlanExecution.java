package org.qcri.rheem.rest.endpoints;

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
import org.qcri.rheem.spark.platform.SparkPlatform;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 *  Resource (exposed at "plan_executions" path)
 */
@Path("plan_executions")
public class PlanExecution {

    private static Logger logger = Logger.getLogger(PlanExecution.class);

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Map create(Map inputJsonObj) throws IOException {

        Map<String, Object> response = new HashMap<>();
        //String res = "{\"run_id\":\"1\",\"stages\":[{\"sequence_number\":0,\"operators\":[{\"is_start\":1,\"is_terminal\":0,\"name\":\"mySource\",\"java_class\":\"org.qcri.rheem.java.operators.JavaTextFileSource\",\"connects_to\":{\"0\":[{\"flatMapOperator\":0,\"via\":\"StreamChannel\"}]}},{\"is_start\":0,\"is_terminal\":0,\"name\":\"flatMapOperator\",\"java_class\":\"org.qcri.rheem.java.operators.JavaFlatMapOperator\",\"connects_to\":{\"0\":[{\"mapOperator\":0,\"via\":\"StreamChannel\"}]}},{\"is_start\":0,\"is_terminal\":0,\"name\":\"mapOperator\",\"java_class\":\"org.qcri.rheem.java.operators.JavaMapOperator\",\"connects_to\":{\"0\":[{\"reduceOp\":0,\"via\":\"StreamChannel\"}]}},{\"is_start\":0,\"is_terminal\":0,\"name\":\"reduceOp\",\"java_class\":\"org.qcri.rheem.java.operators.JavaReduceByOperator\",\"connects_to\":{\"0\":[{\"sinkoperator1\":0,\"via\":\"CollectionChannel\"}]}},{\"is_start\":0,\"is_terminal\":1,\"name\":\"sinkoperator1\",\"java_class\":\"org.qcri.rheem.java.operators.JavaLocalCallbackSink\",\"connects_to\":{}}],\"platform\":\"Java Streams\"}]}";

        try {
            // Instantiate Rheem and activate the backend.
            logger.info("inputJsonObj: " + inputJsonObj);
            System.out.println(inputJsonObj);

            RheemContext rheemContext = new RheemContext();
            rheemContext.register(Java.basicPlugin());
            //rheemContext.register(Spark.basicPlugin());
            RheemPlan rheemPlan = Core.getRheemPlanFromJson(inputJsonObj);
            ExecutionPlan execplan = rheemContext.buildInitialExecutionPlan("1", rheemPlan);
            new Thread("rheemplan1"){
                public void run(){
                    rheemContext.execute(rheemPlan);
                }
            }.start();

            response.put("run_id", "1");
            response.put("stages", execplan.toJsonList());
        }catch (RheemRestException e1) {
            System.out.println(e1);
            logger.error(e1);
            response.put("error", "RheemRestException: " + e1.toString());
           // response.put("run_id", "1");
           // response.put("stages", res);
        }
        catch (Exception e2) {
            System.out.println(e2);
            logger.error(e2);
            response.put("error", "Exception: " +e2.toString());
           // response.put("run_id", "1");
           // response.put("stages", res);
        }
        return response;
    }

}
