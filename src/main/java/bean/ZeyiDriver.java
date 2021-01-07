package bean;

import lombok.Data;
import java.io.Serializable;

/**
 * @author xuweiwei
 * @version 1.0
 * @todo 2021/1/7 10:48:17
 */
@Data
public  class ZeyiDriver implements Serializable {
    private String  deviceId;
    private String  deviceModel;
    private String  deviceName;
    private String  operator;
    private String  connectionType;
    private String  systemType;
    private String  systemVersion;
    private String  appName;
    private String  appVersion;
    private String  userId;
    private String  pageName;
    private String  eventType;
    private String  buttonName;
    private String  createTime;
    private String  id;
    private String  browser;
    private String  ipadress;
    private String  GPSadress;
    private String  dingding_user_code ;

    @Override
    public String toString() {
        return "ZeyiDriver{" +
                "deviceId='" + deviceId + '\'' +
                ", deviceModel='" + deviceModel + '\'' +
                ", deviceName='" + deviceName + '\'' +
                ", operator='" + operator + '\'' +
                ", connectionType='" + connectionType + '\'' +
                ", systemType='" + systemType + '\'' +
                ", systemVersion='" + systemVersion + '\'' +
                ", appName='" + appName + '\'' +
                ", appVersion='" + appVersion + '\'' +
                ", userId='" + userId + '\'' +
                ", pageName='" + pageName + '\'' +
                ", eventType='" + eventType + '\'' +
                ", buttonName='" + buttonName + '\'' +
                ", createTime='" + createTime + '\'' +
                ", id='" + id + '\'' +
                ", browser='" + browser + '\'' +
                ", ipadress='" + ipadress + '\'' +
                ", GPSadress='" + GPSadress + '\'' +
                ", dingding_user_code='" + dingding_user_code + '\'' +
                '}';
    }
}
