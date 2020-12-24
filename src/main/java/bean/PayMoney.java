package bean;

import java.io.Serializable;

public class PayMoney implements Serializable {

    private String uid;
    private String paymoney;
    private String vip_id;
    private String updatetime;
    private String siteid;
    private String dt;
    private String dn;
    private String createtime;

    public String getCreatetime() {
        return createtime;
    }

    public void setCreatetime(String createtime) {
        this.createtime = createtime;
    }

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getPaymoney() {
        return paymoney;
    }

    public void setPaymoney(String paymoney) {
        this.paymoney = paymoney;
    }

    public String getVip_id() {
        return vip_id;
    }

    public void setVip_id(String vip_id) {
        this.vip_id = vip_id;
    }

    public String getUpdatetime() {
        return updatetime;
    }

    public void setUpdatetime(String updatetime) {
        this.updatetime = updatetime;
    }

    public String getSiteid() {
        return siteid;
    }

    public void setSiteid(String siteid) {
        this.siteid = siteid;
    }

    public String getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = dn;
    }

    @Override
    public String toString() {
        return "PayMoney{" +
                "uid='" + uid + '\'' +
                ", paymoney='" + paymoney + '\'' +
                ", vip_id='" + vip_id + '\'' +
                ", updatetime='" + updatetime + '\'' +
                ", siteid='" + siteid + '\'' +
                ", dt='" + dt + '\'' +
                ", dn='" + dn + '\'' +
                ", createtime='" + createtime + '\'' +
                '}';
    }
}

