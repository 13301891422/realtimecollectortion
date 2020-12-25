package util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * @author xuweiwei
 * @version 1.0
 * @todo 2020/12/24 9:32:07
 */
public class ParseJsonData {
    public static JSONObject getJsonData(String data) {
        try {
            return JSONObject.parseObject(data);
        } catch (Exception e) {
            return null;
        }
    }

    public static String getJsonString(Object o) {
        return JSON.toJSONString(o);
    }
}
