package org.qcri.rheem.rest.endpoints;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.rest.config.Config;
import org.qcri.rheem.rest.model.Core;
import org.qcri.rheem.rest.util.ExecPlanUtil;
import org.qcri.rheem.spark.Spark;
import org.qcri.rheem.spark.platform.SparkPlatform;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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
            ExecPlanUtil.deleteRun(new File(Config.RUN_URI));
            final RheemContext rheemContext;
            try {
                rheemContext = new RheemContext(new Configuration(Config.rheemPropertiesUrl));
                rheemContext.register(Java.basicPlugin());
                //rheemContext.register(Spark.basicPlugin());
                RheemPlan rheemPlan = Core.getRheemPlanFromJson(inputJsonObj);
                System.out.println("lalal");
                ExecutionPlan execplan = rheemContext.buildInitialExecutionPlan("1", rheemPlan);
                new Thread("rheemplan1"){
                    public void run(){
                        rheemContext.execute(rheemPlan);
                    }
                }.start();

                List<Map> execplanList = execplan.toJsonList();

                for(Map aMap : execplanList){
                    //System.out.println(aMap);
                    if(aMap.get("operators")!= null){
                        //System.out.println(aMap.get("operators"));
                        List<Map> ops = (List)aMap.get("operators");
                        //System.out.println(ops);

                        if(!ExecPlanUtil.hasOnlyOneStartPoint(ops)){
                            List<Map> update_ops = ExecPlanUtil.sortStartingNodes(ops);
                            aMap.put("operators", update_ops);

                        }
                    }
                }
                response.put("run_id", "1");
                response.put("stages", execplanList);
            } catch (RheemException e){
                System.out.println("explote");
            }






        } catch (Exception e) {
            System.out.println("Error: " + e);
            e.printStackTrace();
            response.put("error", e.toString());
        }
        return response;
    }


}
