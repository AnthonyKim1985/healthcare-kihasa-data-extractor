package org.bigdatacenter.healthcarekihasadataextractor.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;

public class CommonUtil {
    public static boolean isNumeric(String value) {
        try {
            //noinspection ResultOfMethodCallIgnored
            Double.parseDouble(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static String getHashedString(String string) {
        StringBuilder hashedStringBuilder = new StringBuilder();
        try {
            MessageDigest sh = MessageDigest.getInstance("SHA-256");
            sh.update(string.getBytes());

            for (byte aByteData : sh.digest())
                hashedStringBuilder.append(Integer.toString((aByteData & 0xff) + 0x100, 16).substring(1));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
        return hashedStringBuilder.toString();
    }

    public static String getHdfsLocation(String dbAndTableName, Integer dataSetUID) {
        return String.format("/tmp/health_care/%s/%d/%s", dbAndTableName,
                dataSetUID, String.valueOf(new Timestamp(System.currentTimeMillis()).getTime()));
    }
}