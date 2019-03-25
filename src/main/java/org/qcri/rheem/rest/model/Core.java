package org.qcri.rheem.rest.model;

import net.openhft.compiler.CompilerUtils;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OperatorBase;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.*;

public class Core {

    public static HashMap<String, Class> COMPILADAS = new HashMap<>();
    public static Class getClassFromText(String sourceCode, String className) throws Exception {
        if( ! COMPILADAS.containsKey(className)) {
            System.out.println("compilada??  "+ className);
            try {
                Class compilada = CompilerUtils.CACHED_COMPILER.loadFromJava(className, sourceCode);
                COMPILADAS.put(className, compilada);
                System.out.println(COMPILADAS);
            }catch (Exception e){
                e.printStackTrace();
            }

        }
        return COMPILADAS.get(className);

    }

    public static Object parseObjectFromString(String s, Class clazz) throws Exception {
        if (s.equals("null"))
            // Special null case
            return null;
        else return clazz.getConstructor(new Class[] {String.class }).newInstance(s);
    }

    public static Boolean isUdfParam(Class cls) {
        return (FunctionDescriptor.SerializableBinaryOperator.class.isAssignableFrom(cls)
                || FunctionDescriptor.SerializableFunction.class.isAssignableFrom(cls)
                || PredicateDescriptor.SerializablePredicate.class.isAssignableFrom(cls)
                || FunctionDescriptor.class.isAssignableFrom(cls));
    }

    public static RheemPlan getRheemPlanFromJson(Map inputJsonObj) throws Exception{
        int pase = 0;
        ArrayList<HashMap> jsonOperators = (ArrayList)inputJsonObj.get("operators");
        Map<String, Operator> operatorMap = new HashMap<>();


        // create operators.
        for (HashMap jsonOp : jsonOperators){
            Class opClass = Class.forName((String)jsonOp.get("java_class"));
            ArrayList<String> jsonConstrParamValues = new ArrayList(((LinkedHashMap)jsonOp.get("parameters")).values());
            ArrayList<String> jsonConstrParamNames = new ArrayList(((LinkedHashMap)jsonOp.get("parameters")).keySet());
            String opName = (String)jsonOp.get("name");
            Object[] constructorParams = null;
            Operator opObj = null;
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println("OPERATOR::::: "+ opName);

            for (Constructor ctr: opClass.getDeclaredConstructors()) {
                Class[] constrParamTypes = ctr.getParameterTypes();
                System.out.println(constrParamTypes.length);
                System.out.println(jsonConstrParamValues.size());
                System.out.println(ctr);
                if (constrParamTypes.length==jsonConstrParamValues.size()){
                    System.out.println("here??");
                    System.out.println(Arrays.toString(constrParamTypes));
                    try{
                        constructorParams = new Object[ctr.getParameterCount()];
                        for (int k=0; k<ctr.getParameterCount(); k++){
                            System.out.println("lalalal " + constrParamTypes[k]);
                            if (isUdfParam(constrParamTypes[k])) {
                                System.out.println("pase " + (++pase));
                                //flatMapOperator_param0_UdfFactory
                                String udfClassName = jsonConstrParamNames.get(k) + "_UdfFactory";
                                //String udfClassName = "FilterOperator_FilterOperator_UdfFactory";
                                udfClassName = "org.qcri.rheem.rest." + udfClassName;

                                Class udfCls = getClassFromText(jsonConstrParamValues.get(k), udfClassName);
                                Object udfObj;
                                if (udfCls != null) {
                                    Method m = udfCls.getDeclaredMethod("create", null);
                                    udfObj = m.invoke(null, null);
                                } else{
                                    udfObj = null;
                                }
                                constructorParams[k] = udfObj;
                            }
                            else if (constrParamTypes[k].isAssignableFrom(Class.class)){
                                System.out.println("heeehehe");
                                constructorParams[k] = Class.forName(jsonConstrParamValues.get(k));
                            }
                            else {
                                // try to just Parse string value. This will work for any type providing a
                                // constructor that accepts a single string parameter.
                                System.out.println("hdahjkd");
                                System.out.println("jsonConstrParamValues" + Arrays.toString(jsonConstrParamValues.toArray()));
                                System.out.println("jsonConstrParamValues.get(k)" + jsonConstrParamValues.get(k));
                                System.out.println("constrParamTypes[k]" + constrParamTypes[k]);
                                constructorParams[k] = parseObjectFromString(jsonConstrParamValues.get(k),
                                        constrParamTypes[k]);

                            }

                        }
                            opObj = (OperatorBase)ctr.newInstance(constructorParams);
                            opObj.setName(opName);
                            break;

                        }catch (Exception e) {
                            //e.printStackTrace();
                            // e.printStackTrace();
                           /** System.err.println(opClass.getClass().getName());
                            System.err.println("@@@@@"+jsonOp.get("java_class"));
                            System.err.println("types"+Arrays.toString(constrParamTypes));
                            System.err.println("params"+Arrays.toString(constructorParams));
                            **/
                            System.out.println("dashgdhajs");
                            continue;
                        }

                }

            }
            if (opObj==null)
                throw new InstantiationException("Could not find a single valid constructor for operator " + opName + " of type " + opClass.toString());
            else {
                operatorMap.put(opName, opObj);
            }

        }

        // Build plan by connecting operators.
        for (HashMap jsonOp : jsonOperators){
            String thisOpName = (String)jsonOp.get("name");
            Operator thisOp = operatorMap.get(thisOpName);
            HashMap<String, ArrayList<HashMap<String, Integer>>>  jsonConnectsTo = (HashMap)jsonOp.get("connects_to");
            if (jsonConnectsTo!=null) {
                for (String thisOutputIndexStr : jsonConnectsTo.keySet()) {
                    ArrayList<HashMap<String, Integer>> perOutputThatList = jsonConnectsTo.get(thisOutputIndexStr);
                    Integer thisOutputIndex = (Integer) parseObjectFromString(thisOutputIndexStr, Integer.class);
                    for (HashMap<String, Integer> jsonThatOp : perOutputThatList) {
                        // TODO jsonThatOp is hashmap with one entry, this is bad design, should remove the list and just keep
                        // a hashmap per output slot.
                        String thatOpName = jsonThatOp.keySet().iterator().next();
                        Integer thatOutputIndex = jsonThatOp.get(thatOpName);
                        Operator thatOp = operatorMap.get(thatOpName);
                        // Do the connection
                        System.out.println(thisOp);
                        System.out.println(thisOutputIndex);
                        System.out.println(thatOp);
                        System.out.println(thatOutputIndex);

                        thisOp.connectTo(thisOutputIndex, thatOp, thatOutputIndex);
                    }
                }
            }

            HashMap<String, ArrayList<HashMap<String, Integer>>>  jsonbroadCastsTo = (HashMap)jsonOp.get("broadcasts_to");
            if (jsonbroadCastsTo!=null) {
                for (String thisOutputIndexStr : jsonbroadCastsTo.keySet()) {
                    ArrayList<HashMap<String, Integer>> perOutputThatList = jsonbroadCastsTo.get(thisOutputIndexStr);

                    // For now assume the broadcast name is the operator name
                    // TODO: add broadcast name parameter.
                    Integer thisOutputIndex = (Integer) parseObjectFromString(thisOutputIndexStr, Integer.class);
                    String broadCastName = thisOpName;
                    for (HashMap<String, Integer> jsonThatOp : perOutputThatList) {
                        // TODO jsonThatOp is hashmap with one entry, same comment as connect_to above

                        String thatOpName = jsonThatOp.keySet().iterator().next();
                        Operator thatOp = operatorMap.get(thatOpName);
                        // Do the broadcast
                        thisOp.broadcastTo(thisOutputIndex, thatOp, broadCastName);
                    }
                }
            }


        }

        // Detect sinks and create a new rheem plan
        ArrayList<String> jsonSinkOperators = (ArrayList)inputJsonObj.get("sink_operators");
        Operator [] sinks = new Operator[jsonSinkOperators.size()];
        for (int z=0; z<sinks.length; z++) {
            sinks[z] = operatorMap.get(jsonSinkOperators.get(z));
        }
        return new RheemPlan(sinks);
    }


    public static Object[] buildContructorParameter(Constructor ctr, Class[] constrParamTypes,
                                                    String opName, ArrayList<String> jsonConstrParamNames,
                                                    ArrayList<String> jsonConstrParamValues) throws Exception {

        Object[] constructorParams = new Object[ctr.getParameterCount()];

        for (int k=0; k<ctr.getParameterCount(); k++){
            //System.out.println(constrParamTypes[k]);
            if (isUdfParam(constrParamTypes[k])){
                //flatMapOperator_param0_UdfFactory
                String udfClassName = opName + "_" + jsonConstrParamNames.get(k) + "_UdfFactory";
                udfClassName = "org.qcri.rheem.rest." + udfClassName;

                Class udfCls = getClassFromText(jsonConstrParamValues.get(k), udfClassName);
                Method m = udfCls.getDeclaredMethod("create", null);
                Object udfObj = m.invoke(null, null);
                constructorParams[k] = udfObj;
            }
            else if (constrParamTypes[k].isAssignableFrom(Class.class)){
                constructorParams[k] = Class.forName(jsonConstrParamValues.get(k));
            }
            else {
                // try to just Parse string value. This will work for any type providing a
                // constructor that accepts a single string parameter.
                Object obj = parseObjectFromString(jsonConstrParamValues.get(k), constrParamTypes[k]);
                if((obj == null && jsonConstrParamValues.get(k).equalsIgnoreCase("null")) || obj != null) {
                    constructorParams[k] = obj;
                }

            }

        }

        return constructorParams;
    }
}
