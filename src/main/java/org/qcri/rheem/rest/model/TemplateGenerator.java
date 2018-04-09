package org.qcri.rheem.rest.model;

import org.apache.log4j.Logger;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OperatorBase;
import org.qcri.rheem.rest.endpoints.RheemOperator;

import java.lang.reflect.Constructor;
import java.util.*;

/**
 * Created by jlucas on 8/16/17.
 */
public class TemplateGenerator {
    private static Logger logger = Logger.getLogger(TemplateGenerator.class);

    public List<String> getJavaLibaryItems() {
        return javaLibaryItems;
    }

    List<String> javaLibaryItems = new ArrayList<>();
    Map<Integer, String> methodItems = new HashMap<>();
    Map<String, Operator> operatorMap = new HashMap<>();
    Map<String, Param> operatorParamMap = new HashMap<>();
    List<Param> paramList = new ArrayList<>();
    List<Param> orderedOperatorsList = new ArrayList<>();

    public TemplateGenerator() {
        javaLibaryItems.add("import org.qcri.rheem.core.plan.rheemplan.RheemPlan;");
        javaLibaryItems.add("import java.util.ArrayList;");
        javaLibaryItems.add("import java.util.Arrays;");
        javaLibaryItems.add("import java.util.List;");
        javaLibaryItems.add("import org.qcri.rheem.core.types.DataSetType;");
    }

    public List<Param> populateStructure(Map inputJsonObj) throws Exception{

        ArrayList<HashMap> jsonOperators = (ArrayList)inputJsonObj.get("operators");
        // create operators.

        for (HashMap jsonOp : jsonOperators){
            Class opClass = Class.forName((String)jsonOp.get("java_class"));

            this.addToLibaryList(opClass.getName());

            ArrayList<String> jsonConstrParamValues = new ArrayList(((LinkedHashMap)jsonOp.get("parameters")).values());
            ArrayList<String> jsonConstrParamNames = new ArrayList(((LinkedHashMap)jsonOp.get("parameters")).keySet());
            ArrayList<String> jsonConstrParamConnect = null;

            if(jsonOp.get("connects_to") != null){
                HashMap<String, ArrayList<HashMap<String, Integer>>>  jsonConnectsTo = (HashMap)jsonOp.get("connects_to");

                if (jsonConnectsTo!=null) {
                    jsonConstrParamConnect = new ArrayList<String>();
                    for (String thisOutputIndexStr : jsonConnectsTo.keySet()) {
                        ArrayList<HashMap<String, Integer>> perOutputThatList = jsonConnectsTo.get(thisOutputIndexStr);
                        for (HashMap<String, Integer> jsonThatOp : perOutputThatList) {
                            jsonConstrParamConnect.add(jsonThatOp.keySet().iterator().next());
                        }
                    }
                }
            }

            String opName = (String)jsonOp.get("name");
            String opType = (String)jsonOp.get("type");
            String javaClass = (String)jsonOp.get("java_class");

            Operator opObj = this.getOperator(opClass, jsonConstrParamValues, jsonConstrParamNames, opName);

            boolean start = this.isStartPoint(jsonOp, opType);
            boolean last = this.isLastPoint(opType);

            Param paramClass = new Param(opType, opName, false, jsonConstrParamConnect,start, javaClass, last );

            if(opObj != null){
                int index = 0;
                for(String item : jsonConstrParamValues){
                    this.populateLibrary(item);

                    if(index == 0){
                        paramClass.setValue(item);
                        if(item.contains("_UdfFactory")){
                            paramClass.setIsValidUDF(true);
                        }
                        methodItems.put(methodItems.size()+1, item);
                    }

                    index++;
                }
            }

            if(start){
                this.orderedOperatorsList.add(paramClass);
            }

            this.paramList.add(paramClass);
            this.operatorMap.put(opName, opObj);
            this.operatorParamMap.put(opName, paramClass);

        }
        this.sortOperators();
        this.populateNextParamClass();
        return this.orderedOperatorsList;
    }

    private void populateLibrary(String item){
        if(this.isLibrary(item)){
            String[] paramValues = item.split("\n");
            for(int i=0; i < paramValues.length; i++){
                if(this.isLibrary(paramValues[i])){
                    //javaLibaryItems.add(paramValues[i]);
                    this.addToLibaryList(paramValues[i]);
                }
            }
        }
    }

    private void addToLibaryList(String item){

        if(item.indexOf("import TYPE") >= 0){ return;}

        if(!item.trim().startsWith("import")){
            item = "import " + item;
        }

        if(item.trim().startsWith("package")){
            item.replace("package","import");
        }

        if(!item.trim().endsWith(";")){
            item = item + ";";
        }

        if(!javaLibaryItems.contains(item)){
            javaLibaryItems.add(item);
        }

    }

    private Operator getOperator(Class opClass, ArrayList<String> jsonConstrParamValues, ArrayList<String> jsonConstrParamNames, String opName){

        Object[] constructorParams = null;
        Operator opObj = null;

        for (Constructor ctr: opClass.getDeclaredConstructors()) {
            Class[] constrParamTypes = ctr.getParameterTypes();
            if (constrParamTypes.length==jsonConstrParamValues.size()){
                try{
                    constructorParams = Core.buildContructorParameter(ctr, constrParamTypes, opName, jsonConstrParamNames, jsonConstrParamValues);
                    Object os = ctr.newInstance(constructorParams);
                    if(os != null){
                        opObj = (OperatorBase)os;
                        break;
                    }
                }catch (Exception e) {
                    logger.error(e);
                    continue;
                }
            }
        }

        return opObj;
    }


    private boolean isLibrary(String value){
        if(value.trim().contains("import") || value.trim().contains("java.")){
            return true;
        }
        return false;
    }

    private boolean isStartPoint(Map jsonOp, String opType){
        if((int)jsonOp.get("np_inputs") == 0 && opType.equalsIgnoreCase("source")){

            return true;
        }
        return false;
    }

    private boolean isLastPoint(String opType){
        if(opType.equalsIgnoreCase("sink")){

            return true;
        }
        return false;
    }

    private void getNextOperators(ArrayList<String> connectTo) {

        for(int i=0; i < connectTo.size(); i++) {
            Param nextOps = this.operatorParamMap.get(connectTo.get(i));
            this.orderedOperatorsList.add(nextOps);
            if(nextOps.getConnectTo() != null) {
                if (nextOps.getConnectTo().size() > 0) {
                    this.getNextOperators(nextOps.getConnectTo());
                }
            }
        }
    }

    private void sortOperators(){
        this.getNextOperators(this.orderedOperatorsList.get(0).getConnectTo());
    }

    public void populateNextParamClass(){

        for(int i =0; i < this.orderedOperatorsList.size() - 1; i++){
          //  int sinkIndex = this.orderedOperatorsList.size() -1;
          //  if((i+1) < sinkIndex){
                this.orderedOperatorsList.get(i).setNextParam(this.orderedOperatorsList.get(i + 1));
           // }
        }
    }
    public void buildOperators(){
        RheemOperator ops = new RheemOperator();
        List<Map> operators = ops.getOperators();

        operators.forEach((k)->{
            //System.out.println("Item : " + k );
            k.forEach((v, e)->{
                //System.out.println("value : " + v + " , e " + e );
                if(v.toString().equalsIgnoreCase("parameters")){
                    Map<String, List<Map>> params = (Map<String, List<Map>>)e;
                    params.values().forEach((pk) -> {
                        // pk.values().forEach
                        pk.forEach(item -> {
                            ((HashMap<String, String>) item).forEach((a, b) -> {
                                System.out.println("a : b " + a + ":" + b);
                            });
                        });
                    });
                }
            });
        });
    }

    public String buildConnectString(List<Param> currentMethodItems){
        StringBuffer sb = new StringBuffer();
        for(int i=0; i < currentMethodItems.size(); i++){
            if(!currentMethodItems.get(i).getType().equalsIgnoreCase("sink")) {

                sb.append(System.getProperty("line.separator"));

                sb.append(currentMethodItems.get(i).getName().toLowerCase());
                sb.append(".connectTo(0,");
                sb.append(currentMethodItems.get(i).getNextParam().getName().toLowerCase());
                sb.append(",0);");
                sb.append(System.getProperty("line.separator"));
            }
        }

        return sb.toString();

    }

    public String buildReturnString(List<Param> currentMethodItems){
        StringBuffer sb = new StringBuffer();
        for(int i=0; i < currentMethodItems.size(); i++){
            if(currentMethodItems.get(i).getType().equalsIgnoreCase("sink")) {
                sb.append(System.getProperty("line.separator"));
                sb.append(" return new RheemPlan(");
                sb.append(currentMethodItems.get(i).getName().toLowerCase());
                sb.append(");");
                sb.append(System.getProperty("line.separator"));
            }
        }

        return sb.toString();

    }

    public String buildUDFStringOutput(List<Param> currentMethodItems){
        StringBuffer sb = new StringBuffer();
        for(int i=0; i < currentMethodItems.size(); i++){
            if(currentMethodItems.get(i).isValidUDF()) {
                sb.append(System.getProperty("line.separator"));
                sb.append(currentMethodItems.get(i).getJavaClass());
                sb.append(" ");
                sb.append(currentMethodItems.get(i).getName().toLowerCase());
                sb.append(" = (");
                sb.append(currentMethodItems.get(i).getJavaClass());
                sb.append(")");
                sb.append(this.getUDFClassName(currentMethodItems.get(i).getValue()));
                sb.append(".create();");
                sb.append(System.getProperty("line.separator"));
            }
        }

        return sb.toString();
    }

    private String getUDFClassName(String udfClass){
        String temp = udfClass.substring(udfClass.indexOf("public class"), udfClass.length());
        temp = temp.replaceFirst("public class", "");
        int index = temp.indexOf("{");
        String s = temp.substring(0, index);
        return s.trim();
    }
}
