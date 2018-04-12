package org.qcri.rheem.rest.util;

import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: jilucas
 * To change this template use File | Settings | File Templates.
 */
public class JsonWrapper {

    private static Logger logger = Logger.getLogger(JsonWrapper.class);
    private static Gson gson = new Gson();

    public static JSONArray sortJsonByKey(JSONArray json, String key)
    {
        JSONArray sorted = new JSONArray();
        SortedMap map = new TreeMap();

        for (Object o : json) {
            JSONObject tmp = (JSONObject) o;
            map.put(tmp.get(key),tmp);
        }

        Set<String> numbers = map.keySet();

        for (String number : numbers) {
            sorted.add(map.get(number));
        }

        return sorted;
    }

    public static Map<String, Object> jsonToMap(JSONObject json) throws Exception {
        Map<String, Object> retMap = new HashMap<String, Object>();

        if(json != null && !json.isEmpty()) {
            retMap = toMap(json);
        }
        else{
            logger.error("jsonToMap is empty or is null [" + json + "]");
        }
        return retMap;
    }

    public static Map<String, Object> jsonToMap(JSONObject json,  Map<String, Class> inputSchema) throws Exception {
        Map<String, Object> map = new HashMap<String, Object>();
        for (String attrName : inputSchema.keySet()) {
            map.put(attrName, json.get(attrName));
        }

        return map;
    }

    public static Map<String, Object> toMap(JSONObject object) throws Exception {
        Map<String, Object> map = new HashMap<String, Object>();

        Iterator<String> keysItr = object.keySet().iterator();
        while(keysItr.hasNext()) {
            String key = keysItr.next();
            Object value = object.get(key);

            if(value instanceof JSONArray) {
                value = toList((JSONArray) value);
            }

            else if(value instanceof JSONObject) {
                value = toMap((JSONObject) value);
            }
            map.put(key, value);
        }
        return map;
    }

    public static List<Object> toList(JSONArray array) throws Exception {
        List<Object> list = new ArrayList<Object>();
        for(int i = 0; i < array.size(); i++) {
            Object value = array.get(i);
            if(value instanceof JSONArray) {
                value = toList((JSONArray) value);
            }

            else if(value instanceof JSONObject) {
                value = toMap((JSONObject) value);
            }
            list.add(value);
        }
        return list;
    }

    public static JSONObject toJson(Map<String, Object> map) {
        JSONObject jsonObject = new JSONObject();

        for (String key : map.keySet()) {
            try {
                Object obj = map.get(key);
                if (obj instanceof Map) {
                    jsonObject.put(key, toJson((Map) obj));
                }
                else if (obj instanceof List) {
                    jsonObject.put(key, toJson((List) obj));
                }
                else {
                    jsonObject.put(key, map.get(key));
                }
            }
            catch (Exception e) {
                System.out.println(e);
            }
        }

        return jsonObject;
    }

    public static JSONArray toJson(List<Object> list) {
        JSONArray jsonArray = new JSONArray();

        for (Object obj : list) {
            if (obj instanceof Map) {
                jsonArray.add(toJson((Map) obj));
            }
            else if (obj instanceof List) {
                jsonArray.add(toJson((List) obj));
            }
            else {
                jsonArray.add(obj);
            }
        }

        return jsonArray;
    }

    public static JSONArray ObjectEntityListToJson(List<?> list){
        JSONArray jsonArray = new JSONArray();
        for (Object obj : list) {
            jsonArray.add(gson.toJson(obj));
        }
        return jsonArray;
    }

}
