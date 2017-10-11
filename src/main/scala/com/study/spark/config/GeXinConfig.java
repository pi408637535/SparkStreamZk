package com.study.spark.config;

import com.stockemotion.common.utils.PropertiesGetter;

/**
 * Created by piguanghua on 2017/10/10.
 */
public interface GeXinConfig {
    String APP_ID = String.valueOf(PropertiesGetter.getValue("getxin.appid"));;

    String APP_KEY = String.valueOf(PropertiesGetter.getValue("getxin.appkey"));;

    String APP_MASTER_SECRET = String.valueOf(PropertiesGetter.getValue("getxin.master.secret"));;

   String HOST = String.valueOf(PropertiesGetter.getValue("getxin.host"));;

    String LOGO_URL = "http://appdev.stockemotion.com/logo.png";

}
