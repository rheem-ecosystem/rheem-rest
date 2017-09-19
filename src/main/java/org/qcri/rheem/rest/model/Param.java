package org.qcri.rheem.rest.model;

import java.util.ArrayList;

/**
 * Created by jlucas on 8/23/17.
 */
public class Param {

    private String type;
    private String name;
    private String value;


    private String javaClass;
    private boolean isValidUDF;
    private boolean isStart;
    private boolean isLast;

    private ArrayList<String> connectTo;

    private Param nextParam;

    public Param(String type, String name, boolean isValidUDF, ArrayList<String> connectTo, boolean isStart, String javaClass, boolean isLast) {
        this.type = type;
        this.name = name;
        this.isValidUDF = isValidUDF;
        this.connectTo = connectTo;
        this.isStart = isStart;
        this.setJavaClass(javaClass);
        this.isLast = isLast;
    }

    public boolean isLast() {
        return isLast;
    }

    public void setIsLast(boolean isLast) {
        this.isLast = isLast;
    }

    public void setJavaClass(String javaClass) {
        this.javaClass =  javaClass.substring(javaClass.lastIndexOf(".") + 1).trim();
    }

    public String getJavaClass(){
        return this.javaClass;
    }

    public Param getNextParam() {
        return nextParam;
    }

    public void setNextParam(Param nextParam) {
        this.nextParam = nextParam;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public boolean isValidUDF() {
        return isValidUDF;
    }

    public void setIsValidUDF(boolean isValidUDF) {
        this.isValidUDF = isValidUDF;
    }

    public ArrayList<String> getConnectTo() {
        return connectTo;
    }

    public void setConnectTo(ArrayList<String> connectTo) {
        this.connectTo = connectTo;
    }

    public boolean getIsStart() {
        return isStart;
    }

    public void setIsStart(boolean isStart) {
        this.isStart = isStart;
    }


    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        if(this.isValidUDF()){
            String value = this.value.toString().substring(this.value.toString().indexOf("public class"), this.value.toString().length());
            value = value.replace("public class","class");
            sb.append(value);
            sb.append(System.getProperty("line.separator"));
        }
        else
        {
            if(this.isLast){
                /**
                 public TextFileSink(String textFileUrl, Class<T> typeClass)
                 public TextFileSink(String textFileUrl,
                 TransformationDescriptor.SerializableFunction<T, String> formattingFunction,
                 Class<T> typeClass)
                 public TextFileSink(String textFileUrl, TransformationDescriptor<T, String> formattingDescriptor)
                 public TextFileSink(TextFileSink<T> that)

                 List<Tuple2> results = new ArrayList<>();
                 */
                String sinkType = this.value.substring(this.value.lastIndexOf(".") + 1).trim();
                sb.append("List<");
                sb.append(sinkType);
                sb.append("> results = new ArrayList<>();");
                sb.append(System.getProperty("line.separator"));
                sb.append(this.javaClass);
                sb.append("<");
                sb.append(sinkType);
                sb.append("> ");
                sb.append(this.name.toLowerCase());
                sb.append(" = ");
                sb.append("LocalCallbackSink.createCollectingSink(results, DataSetType.createDefault(");
                sb.append(sinkType);
                sb.append(".class));");
                sb.append(System.getProperty("line.separator"));
            }
            else {
                sb.append(this.javaClass);
                sb.append(" ");
                sb.append(this.name.toLowerCase());
                sb.append(" = ");
                sb.append("new ");
                sb.append(this.name);
                sb.append("(\"");
                sb.append(this.value);
                sb.append("\");");
            }
        }
        return sb.toString();
    }
}
