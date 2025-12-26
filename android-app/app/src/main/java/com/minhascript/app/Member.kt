package com.minhascript.app

import org.json.JSONObject


data class Member(
    val nin: String,
    val wassitNo: String,
    val ccp: String,
    val phoneNumber: String,
    val status: String = "جديد"
) {
    fun toJson(): JSONObject {
        return JSONObject()
            .put("nin", nin)
            .put("wassitNo", wassitNo)
            .put("ccp", ccp)
            .put("phoneNumber", phoneNumber)
            .put("status", status)
    }

    companion object {
        fun fromJson(json: JSONObject): Member {
            return Member(
                nin = json.optString("nin"),
                wassitNo = json.optString("wassitNo"),
                ccp = json.optString("ccp"),
                phoneNumber = json.optString("phoneNumber"),
                status = json.optString("status", "جديد")
            )
        }
    }
}
