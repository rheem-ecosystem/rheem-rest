package org.qcri.rheem.rest.endpoints;

import org.qcri.rheem.core.plan.rheemplan.OperatorBase;
import org.qcri.rheem.rest.model.Core;
import org.reflections.Reflections;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;

/**
 *  Resource (exposed at "operators" path)
 */
@Path("execution_operators")
public class ExecutionOperator {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Map getList() throws IOException {
        Map<String, Object> response = new HashMap<>();
        Reflections reflections = new Reflections("org.qcri.rheem", "org.qcri.rheem.postgres.operators");
        Set<Class<? extends org.qcri.rheem.core.plan.rheemplan.ExecutionOperator>> subTypes = reflections.getSubTypesOf(org.qcri.rheem.core.plan.rheemplan.ExecutionOperator.class);
        List<Map> operators = new ArrayList<>();
        try{
            for (Class opClass : subTypes) {
                System.out.println("procesando: "+ opClass.toString());
                Map<String, Object> opMap = new HashMap<>();
                Map<String, List<Map>> opParams = new HashMap<>();
                if (!opClass.isInterface()) {
                    Object[] constructorParams = null;
                    OperatorBase opObj = null;
                    org.qcri.rheem.core.plan.rheemplan.ExecutionOperator exOpObj = null;
                    for (Constructor ctr: opClass.getDeclaredConstructors()) {
                        // TODO: Do not iterate over constructors; pick a working constructor in a smarter way.
                        try {
                            constructorParams = new Object[ctr.getParameterCount()];
                            opObj = (OperatorBase)ctr.newInstance(constructorParams);
                            exOpObj = (org.qcri.rheem.core.plan.rheemplan.ExecutionOperator) opObj;
                            opMap.put("class", opClass.getName());
                            opMap.put("supportBroadcast", opObj.isSupportingBroadcastInputs());
                            opMap.put("nb_inputs", opObj.getNumRegularInputs());
                            opMap.put("nb_outputs", opObj.getNumOutputs());
                            opMap.put("platform", exOpObj.getPlatform().getName());
                            break;
                        }catch (Exception e){
                            System.out.println("heres "+ opClass.toString());
                            continue;
                        }

                    }
                    if (opMap.isEmpty()) {
                        System.out.println("here");
                        // throw new InstantiationException("Could not find a single valid plain constructor for operator " + opClass.toString());
                        System.err.println("Could not find a single valid plain constructor for operator " + opClass.toString());
                    } else {
                        operators.add(opMap);
                        for (int i=0; i<opClass.getDeclaredConstructors().length; i++){
                            Constructor ctr = opClass.getDeclaredConstructors()[i];
                            List<Map> opCtrParams = new ArrayList<>();
                            for (int j=0; j<ctr.getParameterTypes().length; j++){
                                Class ctrParamType = ctr.getParameterTypes()[j];
                                HashMap<String, String> ctrParamMap = new HashMap<>();
                                ctrParamMap.put("name", "param"+j);
                                ctrParamMap.put("type", ctrParamType.getName());
                                if(Core.isUdfParam(ctrParamType))
                                    ctrParamMap.put("inputType", "UDF");
                                else
                                    ctrParamMap.put("inputType", "String");
                                opCtrParams.add(ctrParamMap);
                            }
                            opParams.put("" + i, opCtrParams);
                        }
                        opMap.put("parameters", opParams);
                        System.out.println("heeeeeee");
                    }
                }
            }
            System.out.println("hasta aqui??");
            response.put("operators", operators);
        } catch (Exception e) {
            System.out.println("Explote??");
            System.out.println(e);
            e.printStackTrace();
            response.put("error", e.toString());
        }

        return  response;
    }
}


