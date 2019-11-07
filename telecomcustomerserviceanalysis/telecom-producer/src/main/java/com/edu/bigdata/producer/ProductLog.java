package com.edu.bigdata.producer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 产生数据
 */

public class ProductLog {

    private String startTime = "2017-01-01";
    private String endTime = "2017-12-31";

    // 用于存放待随机的联系人电话
    private List<String> phoneList = new ArrayList<>();

    // 用于存放联系人电话与姓名的映射
    private Map<String, String> phoneNameMap = new HashMap<>();

    public static void main(String[] args) throws InterruptedException {

        if (args == null || args.length <= 0) {
            System.out.println("No arguments");
            return;
        }

        ProductLog productLog = new ProductLog();
        productLog.initPhone();
        productLog.writeLog(args[0]);
    }

    /**
     * 初始化随机的电话号码和姓名
     */
    public void initPhone() {
        phoneList.add("13242820024");
        phoneList.add("14036178412");
        phoneList.add("16386074226");
        phoneList.add("13943139492");
        phoneList.add("18714767399");
        phoneList.add("14733819877");
        phoneList.add("13351126401");
        phoneList.add("13017498589");
        phoneList.add("16058589347");
        phoneList.add("18949811796");
        phoneList.add("13558773808");
        phoneList.add("14343683320");
        phoneList.add("13870632301");
        phoneList.add("13465110157");
        phoneList.add("15382018060");
        phoneList.add("13231085347");
        phoneList.add("13938679959");
        phoneList.add("13779982232");
        phoneList.add("18144784030");
        phoneList.add("18637946280");

        phoneNameMap.put("13242820024", "李雁");
        phoneNameMap.put("14036178412", "卫艺");
        phoneNameMap.put("16386074226", "仰莉");
        phoneNameMap.put("13943139492", "陶欣悦");
        phoneNameMap.put("18714767399", "施梅梅");
        phoneNameMap.put("14733819877", "金虹霖");
        phoneNameMap.put("13351126401", "魏明艳");
        phoneNameMap.put("13017498589", "华贞");
        phoneNameMap.put("16058589347", "华啟倩");
        phoneNameMap.put("18949811796", "仲采绿");
        phoneNameMap.put("13558773808", "卫丹");
        phoneNameMap.put("14343683320", "戚丽红");
        phoneNameMap.put("13870632301", "何翠柔");
        phoneNameMap.put("13465110157", "钱溶艳");
        phoneNameMap.put("15382018060", "钱琳");
        phoneNameMap.put("13231085347", "缪静欣");
        phoneNameMap.put("13938679959", "焦秋菊");
        phoneNameMap.put("13779982232", "吕访琴");
        phoneNameMap.put("18144784030", "沈丹");
        phoneNameMap.put("18637946280", "褚美丽");
    }

    /**
     * 根据传入的时间区间，在此范围内随机产生通话建立的时间
     * 公式：startDate.getTime() + (endDate.getTime() - startDate.getTime()) * Math.random()
     *
     * @param startTime
     * @param endTime
     * @return
     */
    public String randomBuildTime(String startTime, String endTime) {
        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
            Date startDate = sdf1.parse(startTime);
            Date endDate = sdf1.parse(endTime);

            if (endDate.getTime() <= startDate.getTime()) {
                return null;
            }

            long randomTS = startDate.getTime() + (long) ((endDate.getTime() - startDate.getTime()) * Math.random());

            Date resultDate = new Date(randomTS);
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String resultTimeString = sdf2.format(resultDate);

            return resultTimeString;

        } catch (ParseException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 生产数据的形式：13651311090,18611213803,2017-10-17 08:15:20,0360
     */
    public String productLog() {

        String caller = null;
        String callee = null;

        String callerName = null;
        String calleeName = null;

        // 随机获取主叫手机号
        int callerIndex = (int) (Math.random() * phoneList.size()); // [0, 20)
        caller = phoneList.get(callerIndex);
        callerName = phoneNameMap.get(caller);

        // 随机获取被叫手机号
        while (true) {
            int calleeIndex = (int) (Math.random() * phoneList.size()); // [0, 20)
            callee = phoneList.get(calleeIndex);
            calleeName = phoneNameMap.get(callee);

            if (!caller.equals(callee)) {
                break;
            }
        }

        // 随机获取通话建立的时间
        String buildTime = randomBuildTime(startTime, endTime);

        // 随机获取通话的时长
        DecimalFormat df = new DecimalFormat("0000");
        String duration = df.format((int) (30 * 60 * Math.random()));

        StringBuilder sb = new StringBuilder();
        sb.append(caller + ",").append(callee + ",").append(buildTime + ",").append(duration);
        return sb.toString();
    }

    /**
     * 将数据写入到文件中
     */
    public void writeLog(String filePath) {
        try {
            OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(filePath), StandardCharsets.UTF_8);

            while (true) {
                Thread.sleep(200);

                String log = productLog();

                osw.write(log + "\n");
                osw.flush(); // 一定要手动flush，这样能确保每条数据写入到文件一次
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
