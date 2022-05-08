package com.iwill;

import org.apache.commons.codec.binary.Base64;

public class BaseTest {

    public static void main(String[] args) {

        String t = "ChBDdXg5MkwyVDBKeUZhQ1FwEg4xMC4xMjMuMTIzLjEyMxiuxMn%2FhTAgEioYeEd1M0lmWWszY3ZtSm9rTW1OK0tjdz09MgNDUE1CCHRlc3RwY2lkSQAAAAAAAGJAUKSFqAVaBHZpZXdogIjVRHIP7JWE6riw66y87Yuw7IqIegJQQ4oBBG1hcnOQAeuJC5gBtRqgAaSFqAWwAYDlCMIBEFREc2FnaUY2N3lCdk1YNzY%3D";
        System.out.println(Base64.decodeBase64(t));

        System.out.println(onlyTable(findTableName("reporting/db/bpa.ad_group.sql")));
    }

    static String findTableName(String dbScript) {
        int lastDotIdx = dbScript.lastIndexOf(".sql");
        int slashIdx = dbScript.lastIndexOf('/');
        String dbTable = dbScript.substring(slashIdx + 1, lastDotIdx);
        return dbTable;
    }

    private static String onlyTable(String table) {
        return table.replaceAll("^.*\\.", "");
    }
}
