package org.qcri.rheem.rest.endpoints;

import org.qcri.rheem.core.plan.rheemplan.OperatorBase;
import org.qcri.rheem.rest.model.Core;
import org.reflections.Reflections;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 *  Resource (exposed at "operators" path)
 */
@Path("rheem_operators")
public class RheemOperator {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Map getList() {
        ArrayList<Exception> kk = new ArrayList<>();
        Map<String, Object> response = new HashMap<>();
        try {
        Reflections reflections = new Reflections("org.qcri.rheem.basic", "org.qcri.rheem.core.plan.rheemplan");
        Set<Class<? extends OperatorBase>> subTypes = reflections.getSubTypesOf(OperatorBase.class);
        List<Map> operators = new ArrayList<>();
        for (Class opClass : subTypes) {
            kk.clear();
            Map<String, Object> opMap = new HashMap<>();
            Map<String, List<Map>> opParams = new HashMap<>();
            if (opClass.getPackage().getName().equals("org.qcri.rheem.basic.operators")) {
                Object[] constructorParams = null;
                OperatorBase opObj = null;
                for (Constructor ctr: opClass.getDeclaredConstructors()) {
                    // TODO: Do not iterate over constructors; pick a working constructor in a smarter way.
                    try {
                        constructorParams = new Object[ctr.getParameterCount()];
                        opObj = (OperatorBase)ctr.newInstance(constructorParams);
                        opMap.put("class", opClass.getName());
                        opMap.put("supportBroadcast", opObj.isSupportingBroadcastInputs());
                        opMap.put("nb_inputs", opObj.getNumRegularInputs());
                        opMap.put("nb_outputs", opObj.getNumOutputs());
                        break;
                    }catch (Exception e){
                        kk.add(e);
                        continue;
                    }

                }

                if (opMap.isEmpty()) {
                    for (Exception jj: kk) {
                        System.out.print("----");
                        System.out.print(kk.size());
                        System.out.print("====");
                        jj.printStackTrace();
                    }
                    throw new InstantiationException("Could not find a single valid plain constructor for operator " + opClass.toString());
                }
                else {
                    operators.add(opMap);
                    for (int i=0; i<opClass.getDeclaredConstructors().length; i++){
                        Constructor ctr = opClass.getDeclaredConstructors()[i];
                        List<Map> opCtrParams = new ArrayList<>();
                        for (int j=0; j<ctr.getParameterTypes().length; j++){
                            Class ctrParamType = ctr.getParameterTypes()[j];
                            HashMap<String, String> ctrParamMap = new HashMap<>();
                            ctrParamMap.put("name", "param"+j);
                            ctrParamMap.put("type", ctrParamType.getName());
                            if(Core.isUdfParam(ctrParamType)){
                                ctrParamMap.put("inputType", "UDF");
                                ctrParamMap.put("udfTmpl", getUdfTemplate(opClass.getSimpleName(), j));
                            }
                            else
                                ctrParamMap.put("inputType", "String");
                            opCtrParams.add(ctrParamMap);
                        }
                        opParams.put("" + i, opCtrParams);
                    }
                    opMap.put("parameters", opParams);
                }


            }

        }

            response.put("operators", operators);
        }
        catch(Exception e) {
            e.printStackTrace();
            response.put("error", e.toString());
        }
        return response;
    }

    private String getUdfTemplate(String operator, Integer parameterIndex) {
        String template;
        try {
            URL templateUrl = this.getClass().getResource("/UDFTemplates/" + operator + parameterIndex + "Udf.tmpl");
            byte[] encoded = Files.readAllBytes(Paths.get(templateUrl.toURI()));
            template = new String(encoded, Charset.defaultCharset());

        }catch (Exception e) {
            //e.printStackTrace();
            template = "";
        }
        return template;
    }
}


