package org.qcri.rheem.rest.endpoints;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.rest.config.Config;
import org.qcri.rheem.rest.model.Core;
import org.qcri.rheem.spark.platform.SparkPlatform;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 *  Resource (exposed at "plan_executions" path)
 */
@Path("plan_executions")
public class PlanExecution {

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Map create(Map inputJsonObj) throws IOException {
        Map<String, Object> response = new HashMap<>();


        try {
            // Instantiate Rheem and activate the backend.
            RheemContext rheemContext = new RheemContext(new Configuration(Config.rheemPropertiesUrl));
            rheemContext.register(JavaPlatform.getInstance());
            rheemContext.register(SparkPlatform.getInstance());
            RheemPlan rheemPlan = Core.getRheemPlanFromJson(inputJsonObj);
            ExecutionPlan execplan = rheemContext.buildInitialExecutionPlan(rheemPlan);
            rheemContext.execute(rheemPlan);
            Random random = new Random();
            response.put("run_id", random.nextInt(1000000));
            response.put("stages", execplan.toJsonList());
        } catch (Exception e) {
            System.out.println("Error: " + e);
            response.put("error", e.toString());
        }
        return response;
    }
}
