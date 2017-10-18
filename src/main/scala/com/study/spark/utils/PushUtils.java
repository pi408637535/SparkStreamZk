package com.study.spark.utils;

import com.gexin.rp.sdk.base.IPushResult;
import com.gexin.rp.sdk.base.impl.AppMessage;
import com.gexin.rp.sdk.base.impl.ListMessage;
import com.gexin.rp.sdk.base.impl.SingleMessage;
import com.gexin.rp.sdk.base.impl.Target;
import com.gexin.rp.sdk.http.IGtPush;
import com.gexin.rp.sdk.template.LinkTemplate;
import com.gexin.rp.sdk.template.NotificationTemplate;
import com.gexin.rp.sdk.template.TransmissionTemplate;
import com.google.common.collect.Lists;
import com.stockemotion.common.utils.*;
import com.study.spark.config.GeXinConfig;
import com.study.spark.model.gexin.Template;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by piguanghua on 2017/10/10.
 */
public class PushUtils {

    /**
     * 发送链接消息，仅适用安卓设备
     *
     * @param title
     *          消息主题
     * @param message
     *          消息内容
     * @param linkUrl
     *          消息跳转的链接
     * @param tokenList
     *          发送的设备列表
     */
    public static void sendLinkMessage(String title, String message, String linkUrl, List<String> tokenList)
    {
        System.setProperty("gexin.rp.sdk.pushlist.needDetails", "true");
        IGtPush push = new IGtPush(GeXinConfig.HOST, GeXinConfig.APP_KEY, GeXinConfig.APP_MASTER_SECRET);
        LinkTemplate template = Template.getLinkTemplate(title, message, linkUrl);

        ListMessage listMessage = new ListMessage();
        listMessage.setData(template);
        listMessage.setOffline(true);
        listMessage.setOfflineExpireTime(24 * 1000 * 3600);

        List<Target> list = new ArrayList<Target>();

        for (String token : tokenList)
        {
            Target target = new Target();
            target.setAppId(GeXinConfig.APP_ID);
            target.setClientId(token);
            list.add(target);
        }

        String taskId = push.getContentId(listMessage);

        IPushResult ret = push.pushMessageToList(taskId, list);

    }


    /**
     * 推送简单消息
     *
     * @param obj
     * @param tokenList
     *          设备列表
     * @throws JSONException
     */
    public static void sendSimpleAlertMessage(JSONObject obj, List<String> tokenList) throws JSONException
    {
        System.setProperty("gexin.rp.sdk.pushlist.needDetails", "true");
        IGtPush push = new IGtPush(GeXinConfig.HOST, GeXinConfig.APP_KEY, GeXinConfig.APP_MASTER_SECRET);
        TransmissionTemplate template = Template.getSimpleAlertTemplate(obj);

        ListMessage listMessage = new ListMessage();
        listMessage.setData(template);
        listMessage.setOffline(true);
        listMessage.setOfflineExpireTime(24 * 1000 * 3600);

        List<Target> list = new ArrayList<Target>();

        for (String token : tokenList)
        {
            Target target = new Target();
            target.setAppId(GeXinConfig.APP_ID);
            target.setClientId(token);
            list.add(target);
        }

        String taskId = push.getContentId(listMessage);

        IPushResult ret = push.pushMessageToList(taskId, list);

    }


    /**
     * 发送自定义消息，仅适用于安卓设备
     *
     * @param obj
     *          发送消息的JSON对象
     * @param token
     *          设备id
     * @throws JSONException
     */
    public static void sendDictionaryAlertMessage(JSONObject obj, String token) throws JSONException
    {
        System.setProperty("gexin.rp.sdk.pushlist.needDetails", "true");
        IGtPush push = new IGtPush(GeXinConfig.HOST, GeXinConfig.APP_KEY, GeXinConfig.APP_MASTER_SECRET);
        TransmissionTemplate template = Template.getDicAlertTemplate(obj);

        SingleMessage singleMessage = new SingleMessage();
        singleMessage.setPushNetWorkType(0);
        singleMessage.setData(template);
        singleMessage.setOffline(true);
        singleMessage.setOfflineExpireTime(24 * 1000 * 3600);

        Target target = new Target();
        target.setAppId(GeXinConfig.APP_ID);
        target.setClientId(token);

        IPushResult ret = push.pushMessageToSingle(singleMessage, target);

    }

    /**
     * 发送消息，适用于IOS设备
     *
     * @param obj
     * @param badge
     *          图标显示的数字
     * @param tokenList
     *          设备列表
     * @throws JSONException
     */
    public static void sendDictionaryAlertMessage(JSONObject obj, String badge, List<String> tokenList) throws JSONException
    {
        System.setProperty("gexin.rp.sdk.pushlist.needDetails", "true");
        IGtPush push = new IGtPush(GeXinConfig.HOST, GeXinConfig.APP_KEY, GeXinConfig.APP_MASTER_SECRET);
        TransmissionTemplate template = Template.getDicAlertTemplate(obj, badge);

        ListMessage listMessage = new ListMessage();
        listMessage.setData(template);
        listMessage.setOffline(true);
        listMessage.setOfflineExpireTime(24 * 1000 * 3600);

        List<Target> list = new ArrayList<Target>();

        for (String token : tokenList)
        {
            Target target = new Target();
            target.setAppId(GeXinConfig.APP_ID);
            target.setClientId(token);
            list.add(target);
        }

        String taskId = push.getContentId(listMessage);
        IPushResult ret = push.pushMessageToList(taskId, list);

        System.out.println("push message result " + ret.getResponse().toString());
    }

    /**
     * 发送消息，适用于安卓设备
     *
     * @param obj
     * @param tokenList
     *          设备列表
     * @throws JSONException
     */
    public static void sendDictionaryAlertMessage(JSONObject obj, List<String> tokenList) throws JSONException
    {
        System.setProperty("gexin.rp.sdk.pushlist.needDetails", "true");
        IGtPush push = new IGtPush(GeXinConfig.HOST, GeXinConfig.APP_KEY, GeXinConfig.APP_MASTER_SECRET);
        TransmissionTemplate template = Template.getDicAlertTemplate(obj);

        ListMessage listMessage = new ListMessage();
        listMessage.setData(template);
        listMessage.setOffline(true);
        listMessage.setOfflineExpireTime(24 * 1000 * 3600);

        List<Target> list = new ArrayList<Target>();

        for (String token : tokenList)
        {
            Target target = new Target();
            target.setAppId(GeXinConfig.APP_ID);
            target.setClientId(token);
            list.add(target);
        }

        String taskId = push.getContentId(listMessage);

        IPushResult ret = push.pushMessageToList(taskId, list);

    }

    /**
     * 通知消息，仅适用于安卓设备
     *
     * @param title
     *          消息主题
     * @param message
     *          消息内容
     * @param tokenList
     *          发送的设备列表
     */
    public static void sendNotificationMessage(String title, String message, List<String> tokenList)
    {
        System.setProperty("gexin.rp.sdk.pushlist.needDetails", "true");
        IGtPush push = new IGtPush(GeXinConfig.HOST, GeXinConfig.APP_KEY, GeXinConfig.APP_MASTER_SECRET);
        NotificationTemplate template = Template.getNotificationTemplate(title, message);

        ListMessage listMessage = new ListMessage();
        listMessage.setData(template);
        listMessage.setOffline(true);
        listMessage.setOfflineExpireTime(24 * 1000 * 3600);

        List<Target> list = new ArrayList<Target>();

        for (String token : tokenList)
        {
            Target target = new Target();
            target.setAppId(GeXinConfig.APP_ID);
            target.setClientId(token);
            list.add(target);
        }

        String taskId = push.getContentId(listMessage);

        IPushResult ret = push.pushMessageToList(taskId, list);

        System.out.println("push message result " + ret.getResponse().toString());
    }

    public static void sendSimpleAlertMessage(JSONObject obj, String badge, List<String> tokenList) throws JSONException
    {
        System.setProperty("gexin.rp.sdk.pushlist.needDetails", "true");
        IGtPush push = new IGtPush(GeXinConfig.HOST, GeXinConfig.APP_KEY, GeXinConfig.APP_MASTER_SECRET);
        TransmissionTemplate template = Template.getSimpleAlertTemplate(obj, badge);
        //    NotificationTemplate template = Template.getSimpleAlertTemplatePi(obj, badge);

        ListMessage listMessage = new ListMessage();
        listMessage.setData(template);
        listMessage.setOffline(true);
        listMessage.setOfflineExpireTime(24 * 1000 * 3600);

        List<Target> list = new ArrayList<Target>();

        for (String token : tokenList)
        {
            Target target = new Target();
            target.setAppId(GeXinConfig.APP_ID);
            target.setClientId(token);
            list.add(target);
        }

        String taskId = push.getContentId(listMessage);

        IPushResult ret = push.pushMessageToList(taskId, list);

        System.out.println("push message result " + ret.getResponse().toString());
    }


    /**
     * 发送简单消息，适用于IOS设备
     *
     * @param obj
     *          发送消息JSON对象
     * @param badge
     *          应用图标右上角数字
     * @throws JSONException
     */
    public static void sendSimpleAlertMessageForIOS(JSONObject obj, String badge) throws JSONException
    {
        System.setProperty("gexin.rp.sdk.pushlist.needDetails", "true");
        IGtPush push = new IGtPush(GeXinConfig.HOST, GeXinConfig.APP_KEY, GeXinConfig.APP_MASTER_SECRET);
        TransmissionTemplate template = Template.getSimpleAlertTemplate(obj, badge);

        AppMessage appMessage = new AppMessage();
        appMessage.setData(template);
        appMessage.setOffline(true);
        appMessage.setOfflineExpireTime(24 * 1000 * 3600);
        List<String> appIdList = new ArrayList<String>();
        appIdList.add(GeXinConfig.APP_ID);
        appMessage.setAppIdList(appIdList);
        IPushResult ret = push.pushMessageToApp(appMessage);

        System.out.println("push message result " + ret.getResponse().toString());
    }

    /**
     * 发送字典消息，适用于IOS
     *
     * @param obj
     *          发送消息JSON对象
     * @param badge
     *          图标右上角数字
     * @throws JSONException
     */
    public static void sendDictionaryAlertMessageForIOS(JSONObject obj, String badge) throws JSONException
    {
        System.setProperty("gexin.rp.sdk.pushlist.needDetails", "true");
        IGtPush push = new IGtPush(GeXinConfig.HOST, GeXinConfig.APP_KEY, GeXinConfig.APP_MASTER_SECRET);
        TransmissionTemplate template = Template.getDicAlertTemplate(obj, badge);

        AppMessage appMessage = new AppMessage();
        appMessage.setData(template);
        appMessage.setOffline(true);
        appMessage.setOfflineExpireTime(24 * 1000 * 3600);
        List<String> appIdList = new ArrayList<String>();
        appIdList.add(GeXinConfig.APP_ID);
        appMessage.setAppIdList(appIdList);

        IPushResult ret = push.pushMessageToApp(appMessage);

        System.out.println("push message result " + ret.getResponse().toString());
    }

    public static void sendElfPushMessage(String stockCode, String stockName,String content, String clientId,String message, byte deviceType){
        JSONObject messageJson = new JSONObject();
        try {
            messageJson.put("title", "沃德股市气象站");
            messageJson.put("message", message);
            messageJson.put("content", content);
            // 股票代码
            messageJson.put("stockCode", stockCode);

            // 股票名称
            messageJson.put("stockName", stockName);

            // 跳转到股票详情
            messageJson.put("action", "3");

            // 消息信息的id
            messageJson.put("notificationId", "2");

            if(deviceType == 0){ //android
                List<String> stringList = new ArrayList<>();
                stringList.add(clientId);
                PushUtils.sendSimpleAlertMessage(messageJson, stringList);
            }else { //ios
                // 跳转到股票详情
                List<String> tokenList = Lists.newArrayList();
                tokenList.add(clientId);
                PushUtils.sendSimpleAlertMessage(messageJson, "+1", tokenList);
            }


        } catch (JSONException e) {
            e.printStackTrace();
        }

    }
}
