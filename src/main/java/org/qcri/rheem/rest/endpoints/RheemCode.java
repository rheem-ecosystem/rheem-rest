package org.qcri.rheem.rest.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.rest.exception.RheemRestException;
import org.qcri.rheem.rest.model.Core;
import org.qcri.rheem.rest.model.Param;
import org.qcri.rheem.rest.model.TemplateGenerator;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  Resource (exposed at "plans" path)
 */
@Path("code")
public class RheemCode {
    private static Logger logger = Logger.getLogger(RheemCode.class);

    @POST @Path("/java")
    @Produces(MediaType.APPLICATION_JSON)
    public Map getJava(Map inputJsonObj) throws Exception {
        Map<String, Object> response = new HashMap<>();

        if(inputJsonObj == null){
            response.put("error", "Make sure that you select a plan!");
            return response;
        }

        try {
            TemplateGenerator tg = new TemplateGenerator();
            List<Param> methodItems = tg.populateStructure(inputJsonObj);

            //methodItems.forEach(System.out::println);

            StringBuffer sb = new StringBuffer();

            tg.getJavaLibaryItems().forEach(item -> {
                sb.append(item);
                sb.append(System.getProperty("line.separator"));
            });
            sb.append(System.getProperty("line.separator"));
            sb.append("public class RheenStudioTemplate {");
            sb.append(System.getProperty("line.separator"));
            sb.append(System.getProperty("line.separator"));
            sb.append("public RheemPlan test(){");
            sb.append(System.getProperty("line.separator"));
            //buildUDFStringOutput
            methodItems.forEach(item -> {
                if (!item.isValidUDF()) {
                    sb.append(item.toString());
                    sb.append(System.getProperty("line.separator"));
                }

            });
            sb.append(System.getProperty("line.separator"));
            sb.append(tg.buildUDFStringOutput(methodItems));
            sb.append(System.getProperty("line.separator"));
            sb.append(tg.buildConnectString(methodItems));
            sb.append(System.getProperty("line.separator"));
            sb.append(tg.buildReturnString(methodItems));
            sb.append(System.getProperty("line.separator"));
            sb.append("}");

            sb.append("}");
            sb.append(System.getProperty("line.separator"));
            methodItems.forEach(item -> {
                if(item.isValidUDF()) {
                    sb.append(item.toString());
                    sb.append(System.getProperty("line.separator"));
                }

            });

            System.out.println(sb.toString());
            response.put("stages", sb.toString());

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
