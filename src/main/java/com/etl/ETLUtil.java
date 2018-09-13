package com.etl;

/**
 * 1.过滤数据
 * 2.去掉 视频类别 字段中的空格
 * 3.替换关联视频的分割符
 */
public class ETLUtil {
    public static String etlStr(String line) {
        // 切割数据
        String[] splits = line.split("\t");
        // 1.过滤脏数据
        if (splits.length < 9) return null;
        // 2.去掉 视频类别 字段中的空格
        splits[3] = splits[3].replaceAll(" ", "");
        // 3.替换关联视频的分割符
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < splits.length; i ++) {
            if (i < 9) {
                if (i == splits.length - 1) {
                    sb.append(splits[i]);
                } else {
                    sb.append(splits[i]).append("\t");
                }
            } else {
                if (i == splits.length - 1) {
                    sb.append(splits[i]);
                } else {
                    sb.append(splits[i]).append("&");
                }
            }
        }
        return sb.toString();
    }
}
