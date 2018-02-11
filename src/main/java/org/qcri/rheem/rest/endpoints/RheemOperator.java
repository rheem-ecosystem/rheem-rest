package org.qcri.rheem.rest.endpoints;

import org.qcri.rheem.core.plan.rheemplan.OperatorBase;
import org.qcri.rheem.rest.config.BasicOperator;
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
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
/**
 *  Resource (exposed at "operators" path)
 */
@Path("rheem_operators")
public class RheemOperator {

    private static Logger logger = Logger.getLogger(RheemOperator.class);

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Map getList() {
        ArrayList<Exception> kk = new ArrayList<>();
        Map<String, Object> response = new HashMap<>();
        try {
            Reflections reflections = new Reflections("org.qcri.rheem.basic", "org.qcri.rheem.core.plan.rheemplan");

            Set<Class<? extends OperatorBase>> subTypes = reflections.getSubTypesOf(OperatorBase.class)
                                .stream()
                                .filter(a -> a.getPackage().getName().equals("org.qcri.rheem.basic.operators"))
                                .collect(Collectors.toSet());

            List<Map> operators = new ArrayList<>();

            for (Class opClass : subTypes) {
                kk.clear();
                Map<String, Object> opMap = new HashMap<>();
                Map<String, List<Map>> opParams = new HashMap<>();

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
                        opMap.put("loop", opObj.isLoopHead());
                        opMap.put("basic", this.isBasicOperator(opClass.getName()));
                        break;
                    }catch (Exception e){
                        kk.add(e);
                        logger.error(e.getMessage());
                        continue;
                    }
                }

                if (opMap.isEmpty()) {
                    this.iternateError(kk, opClass);
                }
                else {
                    operators.add(opMap);
                    opParams = this.populateConstructParameters(opClass, opParams);
                    opMap.put("parameters", opParams);
                }

            }
            response.put("operators", operators);
        }
        catch(Exception e) {
            logger.error(e);
            response.put("error", e.toString());
        }
        return response;
    }

    private boolean isBasicOperator(String className){
        boolean found = false;
        for(BasicOperator operator : BasicOperator.values()){
            if(operator.getName().equalsIgnoreCase(className)){
                found = true;
            }
        }
        return found;
    }

    private Map<String, List<Map>> populateConstructParameters(Class opClass, Map<String, List<Map>> opParams){

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
                    ctrParamMap.put("udfTmpl", this.getUdfTemplate(opClass.getSimpleName(), j));
                }
                else
                    ctrParamMap.put("inputType", "String");

                opCtrParams.add(ctrParamMap);
            }

            opParams.put("" + i, opCtrParams);
        }

        return opParams;
    }

    private String getUdfTemplate(String operator, Integer parameterIndex) {
        String template;
        try {
            URL templateUrl = this.getClass().getResource("/UDFTemplates/" + operator + parameterIndex + "Udf.tmpl");
            byte[] encoded = Files.readAllBytes(Paths.get(templateUrl.toURI()));
            template = new String(encoded, Charset.defaultCharset());

        }catch (Exception e) {
            logger.error(e);
            template = "";
        }
        return template;
    }

    private void iternateError(ArrayList<Exception> kk, Class opClass) throws Exception{

        for (Exception jj: kk) {
            System.out.print("----");
            System.out.print(kk.size());
            System.out.print("====");
            jj.printStackTrace();
        }
        logger.error("Could not find a single valid plain constructor for operator " + opClass.toString());
        throw new InstantiationException("Could not find a single valid plain constructor for operator " + opClass.toString());

    }

    public List<Map> getOperators(){
        ArrayList<Exception> kk = new ArrayList<>();
        List<Map> operators = new ArrayList<>();
        try {
            Reflections reflections = new Reflections("org.qcri.rheem.basic", "org.qcri.rheem.core.plan.rheemplan");

            Set<Class<? extends OperatorBase>> subTypes = reflections.getSubTypesOf(OperatorBase.class)
                    .stream()
                    .filter(a -> a.getPackage().getName().equals("org.qcri.rheem.basic.operators"))
                    .collect(Collectors.toSet());



            for (Class opClass : subTypes) {
                kk.clear();
                Map<String, Object> opMap = new HashMap<>();
                Map<String, List<Map>> opParams = new HashMap<>();

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
                        opMap.put("loop", opObj.isLoopHead());
                        opMap.put("basic", this.isBasicOperator(opClass.getName()));
                        break;
                    }catch (Exception e){
                        kk.add(e);
                        logger.error(e.getMessage());
                        continue;
                    }
                }

                if (opMap.isEmpty()) {
                    this.iternateError(kk, opClass);
                }
                else {
                    operators.add(opMap);
                    opParams = this.populateConstructParameters(opClass, opParams);
                    opMap.put("parameters", opParams);
                }

            }
        }
        catch(Exception e) {
            logger.error(e);
        }

        return operators;
    }
}


