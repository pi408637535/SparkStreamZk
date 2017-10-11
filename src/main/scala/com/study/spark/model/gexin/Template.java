package com.study.spark.model.gexin;

import com.gexin.rp.sdk.base.payload.APNPayload;
import com.gexin.rp.sdk.base.payload.APNPayload.DictionaryAlertMsg;
import com.gexin.rp.sdk.base.payload.APNPayload.SimpleAlertMsg;
import com.gexin.rp.sdk.template.LinkTemplate;
import com.gexin.rp.sdk.template.NotificationTemplate;
import com.gexin.rp.sdk.template.TransmissionTemplate;
import com.gexin.rp.sdk.template.style.Style0;
import com.study.spark.config.GeXinConfig;
import org.json.JSONException;
import org.json.JSONObject;

public class Template
{
    public static LinkTemplate getLinkTemplate(String title, String message, String linkUrl)
    {
        LinkTemplate template = new LinkTemplate();

        template.setAppId(GeXinConfig.APP_ID);
        template.setAppkey(GeXinConfig.APP_KEY);

        template.setTitle(title);
        template.setText(message);
        template.setLogo("logo.png");
        template.setLogoUrl(GeXinConfig.LOGO_URL);
        template.setIsRing(true);
        template.setIsVibrate(true);
        template.setIsClearable(true);
        template.setUrl(linkUrl);

        return template;
    }

    public static NotificationTemplate getNotificationTemplate(String title, String message)
    {
        NotificationTemplate template = new NotificationTemplate();

        template.setAppId(GeXinConfig.APP_ID);
        template.setAppkey(GeXinConfig.APP_KEY);

        template.setTitle(title);
        template.setText(message);

        template.setLogo("logo.png");

        template.setLogoUrl(GeXinConfig.LOGO_URL);

        template.setIsRing(true);
        template.setIsVibrate(true);
        template.setIsClearable(true);
        template.setTransmissionType(1);
        template.setTransmissionContent(message);

        return template;

    }

    public static NotificationTemplate getSimpleAlertTemplatePi(JSONObject contentObj, String badge) throws JSONException{
        NotificationTemplate template = new NotificationTemplate();
        // 设置APPID与APPKEY
        template.setAppId(GeXinConfig.APP_ID);
        template.setAppkey(GeXinConfig.APP_KEY);

        Style0 style = new Style0();
        // 设置通知栏标题与内容
        style.setTitle("请输入通知栏标题");
        style.setText("请输入通知栏内容");
        // 配置通知栏图标
        style.setLogo("icon.png");
        // 配置通知栏网络图标
        style.setLogoUrl("");
        // 设置通知是否响铃，震动，或者可清除
        style.setRing(true);
        style.setVibrate(true);
        template.setStyle(style);

        // 透传消息设置，1为强制启动应用，客户端接收到消息后就会立即启动应用；2为等待应用启动
        template.setTransmissionType(2);
        template.setTransmissionContent("请输入您要透传的内容");
        return template;
    }



    public static TransmissionTemplate getSimpleAlertTemplate(JSONObject contentObj, String badge) throws JSONException
    {
        TransmissionTemplate template = new TransmissionTemplate();
        template.setAppId(GeXinConfig.APP_ID);
        template.setAppkey(GeXinConfig.APP_KEY);
        // 设置透传数据
        template.setTransmissionContent(contentObj.toString());
        template.setTransmissionType(2);

        APNPayload payload = new APNPayload();
        // 透传带有数据
        payload.setContentAvailable(1);
        payload.setAutoBadge(badge);

        payload.addCustomMsg("stockCode",contentObj.getString("stockCode"));
        payload.addCustomMsg("stockName",contentObj.getString("stockName"));
        payload.addCustomMsg("action","3");


        payload.setContentAvailable(2048);

        //  payload.setCategory(contentObj.getString("stockCode") + "&" + contentObj.getString("stockName"));


        SimpleAlertMsg alertMsg = getSimpleAlertMsg(contentObj.getString("message"));
        payload.setAlertMsg(alertMsg);

        template.setAPNInfo(payload);

        return template;
    }

    public static TransmissionTemplate getSimpleAlertTemplate(JSONObject contentObj) throws JSONException
    {
        TransmissionTemplate template = new TransmissionTemplate();
        template.setAppId(GeXinConfig.APP_ID);
        template.setAppkey(GeXinConfig.APP_KEY);
        // 设置透传数据
        template.setTransmissionContent(contentObj.toString());
        template.setTransmissionType(2);

        return template;
    }

    public static TransmissionTemplate getDicAlertTemplate(JSONObject contentObj, String badge) throws JSONException
    {
        TransmissionTemplate template = new TransmissionTemplate();
        template.setAppId(GeXinConfig.APP_ID);
        template.setAppkey(GeXinConfig.APP_KEY);

        System.out.println("------" + contentObj.toString());
        template.setTransmissionContent(contentObj.toString());
        template.setTransmissionType(2);

        APNPayload payload = new APNPayload();
        payload.setAutoBadge(badge);
        payload.setContentAvailable(1);

        DictionaryAlertMsg alertMsg = getDictionaryAlertMsg(contentObj.getString("message"), contentObj.getString("title"));
        payload.setAlertMsg(alertMsg);

        template.setAPNInfo(payload);

        return template;
    }

    public static TransmissionTemplate getDicAlertTemplate(JSONObject obj) throws JSONException
    {
        TransmissionTemplate template = new TransmissionTemplate();
        template.setAppId(GeXinConfig.APP_ID);
        template.setAppkey(GeXinConfig.APP_KEY);
        template.setTransmissionContent(obj.toString());
        template.setTransmissionType(2);

        return template;
    }

    private static SimpleAlertMsg getSimpleAlertMsg(String message)
    {
        SimpleAlertMsg alertMsg = new SimpleAlertMsg(message);
        return alertMsg;
    }

    private static DictionaryAlertMsg getDictionaryAlertMsg(String messsage, String title)
    {
        DictionaryAlertMsg alertMsg = new DictionaryAlertMsg();

        alertMsg.setBody(messsage);
        // alertMsg.setActionLocKey("ActionLockey");
        // alertMsg.setLocKey("LocKey");
        // alertMsg.addLocArg("loc-args");
        // alertMsg.setLaunchImage("launch-image");

        // IOS8.2以上版本支持
        alertMsg.setTitle(title);
        // alertMsg.setTitleLocKey("TitleLocKey");
        // alertMsg.addTitleLocArg("TitleLocArg");

        return alertMsg;
    }
}
