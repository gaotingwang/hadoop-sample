package com.gtw.hadoop.test.mr.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinInfo implements Writable {

    private String userId;
    private String userName;
    private String orgCode;
    private String orgName;

    /**
     * 标志位，用来区分数据来自于哪个表
     */
    private String flag;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(userId);
        dataOutput.writeUTF(userName);
        dataOutput.writeUTF(orgCode);
        dataOutput.writeUTF(orgName);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.userId = dataInput.readUTF();
        this.userName = dataInput.readUTF();
        this.orgCode = dataInput.readUTF();
        this.orgName = dataInput.readUTF();
        this.flag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return userId + ',' +userName + ',' +orgCode + ',' +orgName + ',' +flag;
    }

    public JoinInfo() {
    }

    public JoinInfo(String orgCode, String orgName, String flag) {
        this.orgCode = orgCode;
        this.orgName = orgName;
        this.flag = flag;
    }

    public JoinInfo(String userId, String userName, String orgCode, String flag) {
        this.userId = userId;
        this.userName = userName;
        this.orgCode = orgCode;
        this.flag = flag;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getOrgCode() {
        return orgCode;
    }

    public void setOrgCode(String orgCode) {
        this.orgCode = orgCode;
    }

    public String getOrgName() {
        return orgName;
    }

    public void setOrgName(String orgName) {
        this.orgName = orgName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }
}
