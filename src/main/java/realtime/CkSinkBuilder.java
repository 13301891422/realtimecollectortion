package realtime;

import bean.ZeyiDriver;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author xuweiwei
 * @version 1.0
 * @todo 2020/12/10 13:02:41
 */
class CkSinkBuilder implements JdbcStatementBuilder<ZeyiDriver> {

    @Override
    public void accept(PreparedStatement ps, ZeyiDriver zeyiDriver) throws SQLException {
        //判null赋空字符串操作
        if (null != zeyiDriver.getDeviceId()){
            ps.setString(1,  zeyiDriver.getDeviceId());
        } else {
            ps.setString(1, "");
        }

        if (null != zeyiDriver.getDeviceModel()) {
            ps.setString(2,  zeyiDriver.getDeviceModel());
        } else {
            ps.setString(2, "");
        }

        if (null != zeyiDriver.getDeviceName()) {
            ps.setString(3,  zeyiDriver.getDeviceName());
        } else {
            ps.setString(3, "");
        }

        if (null != zeyiDriver.getOperator()) {
            ps.setString(4,  zeyiDriver.getOperator());
        } else {
            ps.setString(4, "");
        }

        if (null != zeyiDriver.getConnectionType()) {
            ps.setString(5,  zeyiDriver.getConnectionType());
        } else {
            ps.setString(5, "");
        }

        if (null != zeyiDriver.getSystemType()) {
            ps.setString(6,  zeyiDriver.getSystemType());
        } else {
            ps.setString(6, "");
        }

        if (null != zeyiDriver.getSystemVersion()) {
            ps.setString(7,  zeyiDriver.getSystemVersion());
        } else {
            ps.setString(7, "");
        }

        if (null != zeyiDriver.getAppName()) {
            ps.setString(8,  zeyiDriver.getAppName());
        } else {
            ps.setString(8, "");
        }

        if (null != zeyiDriver.getAppVersion()) {
            ps.setString(9,  zeyiDriver.getAppVersion());
        } else {
            ps.setString(9, "");
        }

        if (null != zeyiDriver.getUserId()) {
            ps.setString(10, zeyiDriver.getUserId());
        } else {
            ps.setString(10, "");
        }

        if (null != zeyiDriver.getPageName()) {
            ps.setString(11, zeyiDriver.getPageName());
        } else {
            ps.setString(11, "");
        }

        if (null != zeyiDriver.getEventType()) {
            ps.setString(12, zeyiDriver.getEventType());
        } else {
            ps.setString(12, "");
        }

        if (null != zeyiDriver.getButtonName()) {
            ps.setString(13, zeyiDriver.getButtonName());
        } else {
            ps.setString(13, "");
        }

        if (null != zeyiDriver.getCreateTime()) {
            ps.setString(14, zeyiDriver.getCreateTime());
        } else {
            ps.setString(14, "");
        }

        if (null != zeyiDriver.getId()) {
            ps.setString(15, zeyiDriver.getId());
        } else {
            ps.setString(15, "");
        }

        if (null != zeyiDriver.getBrowser()) {
            ps.setString(16, zeyiDriver.getBrowser());
        } else {
            ps.setString(16, "");
        }

        if (null != zeyiDriver.getIpadress()) {
            ps.setString(17, zeyiDriver.getIpadress());
        } else {
            ps.setString(17, "");
        }

        if (null != zeyiDriver.getGPSadress()) {
            ps.setString(18, zeyiDriver.getGPSadress());
        } else {
            ps.setString(18, "");
        }

        if (null != zeyiDriver.getDingding_user_code()) {
            ps.setString(19, zeyiDriver.getDingding_user_code());
        } else {
            ps.setString(19, "");
        }
    }
}
