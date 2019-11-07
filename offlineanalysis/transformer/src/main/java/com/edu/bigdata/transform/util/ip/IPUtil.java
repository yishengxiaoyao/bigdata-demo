package com.edu.bigdata.transform.util.ip;

public class IPUtil {
    // 将 127.0.0.1 形式的 IP 地址转换成十进制整数
    public long IpToLong(String strIp) {
        long[] ip = new long[4];
        int position1 = strIp.indexOf(".");
        int position2 = strIp.indexOf(".", position1 + 1);
        int position3 = strIp.indexOf(".", position2 + 1);
        // 将每个.之间的字符串转换成整型
        ip[0] = Long.parseLong(strIp.substring(0, position1));
        ip[1] = Long.parseLong(strIp.substring(position1 + 1, position2 - position1 - 1));
        ip[2] = Long.parseLong(strIp.substring(position2 + 1, position3 - position2 - 1));
        ip[3] = Long.parseLong(strIp.substring(position3 + 1));
        // 进行左移位处理
        return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
    }

    // 将十进制整数形式转换成 127.0.0.1 形式的 ip 地址
    public String LongToIp(long ip) {
        StringBuilder sb = new StringBuilder();
        // 直接右移 24 位
        sb.append(ip >> 24);
        sb.append(".");
        // 将高 8 位置 0，然后右移 16
        sb.append((ip & 0x00FFFFFF) >> 16);
        sb.append(".");
        // 将高 16 位置0 ，然后右移 8 位
        sb.append((ip & 0x0000FFFF) >> 8);
        sb.append(".");
        // 将高 24 位置 0
        sb.append((ip & 0x000000FF));
        return sb.toString();
    }
}
