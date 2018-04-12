package org.qcri.rheem.rest.util;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by jlucas on 4/11/18.
 */
public class ExecPlanUtil {

    public static List<Map> sortStartingNodes(List<Map> ops) {
        List<Map> list = new ArrayList<Map>();
        ArrayList<String> added = new ArrayList<String>();

        for (int i = 0; i < ops.size(); i++) {
            Map aOps = ops.get(i);
            if (aOps.get("is_start") != null) {
                int is_start = Integer.parseInt(aOps.get("is_start").toString());
                if (is_start == 1) {

                    // 1. add a starting node
                    list.add(aOps);

                    added.add(aOps.get("name").toString());

                    Map connection = (Map) aOps.get("connects_to");
                    ArrayList<HashMap> connect_values = (ArrayList<HashMap>) connection.get("0");

                    Map aValue = (Map) connect_values.get(0);

                    Iterator iter = aValue.entrySet().iterator();

                    while (iter.hasNext()) {
                        Map.Entry mEntry = (Map.Entry) iter.next();
                        System.out.println(mEntry.getKey() + " : " + mEntry.getValue());

                        String connect_to_name = mEntry.getKey().toString();

                        // 2. add a target node.
                        Map targetNode = getTargetNode(ops, connect_to_name);

                        if(targetNode != null){
                            added.add(targetNode.get("name").toString());
                            list.add(targetNode);
                        }
                    }

                }
            }
        }

        for (int i = 0; i < ops.size(); i++) {
            Map aOps = ops.get(i);
            String aName = aOps.get("name").toString();
            if(!added.contains(aName)){
                list.add(aOps);
            }
        }
        return list;

    }

    public static Map getTargetNode(List<Map> ops, String target){
        Map targetNode = null ;
        for (int i = 0; i < ops.size(); i++) {
            Map aOps = ops.get(i);
            String name = aOps.get("name").toString();

            if(target.equalsIgnoreCase(name)){
                targetNode = aOps;
            }
        }

        return targetNode;
    }

    public static boolean hasOnlyOneStartPoint(List<Map> ops){
        boolean is_true = true;
        int is_start_count = 0;
        for(int i=0; i < ops.size(); i++){
            Map aOps = ops.get(i);
            if(aOps.get("is_start") != null){
                int is_start = Integer.parseInt(aOps.get("is_start").toString());
                if(is_start == 1){
                    is_start_count = is_start_count + 1;
                }
            }
        }

        if(is_start_count > 1){
            is_true = false;
        }

        return is_true;

    }

    public static void deleteRun(File file) throws IOException {

        if(file == null){
            return;
        }

        if(file.isDirectory()){
            if(file.list().length==0){

                file.delete();

            }
            else{
                String files[] = file.list();

                for (String temp : files) {
                    //construct the file structure
                    File fileDelete = new File(file, temp);

                    //recursive delete
                    deleteRun(fileDelete);
                }

                //check the directory again, if empty then delete it
                if(file.list().length==0){
                    file.delete();
                }
            }

        }else{
            file.delete();
        }
    }
}
