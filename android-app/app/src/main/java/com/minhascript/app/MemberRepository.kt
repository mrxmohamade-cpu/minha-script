package com.minhascript.app

import android.content.Context
import org.json.JSONArray
import java.io.File

class MemberRepository(private val context: Context) {
    private val storageFile = File(context.filesDir, "members.json")

    fun loadMembers(): MutableList<Member> {
        if (!storageFile.exists()) {
            return mutableListOf()
        }

        val content = storageFile.readText().trim()
        if (content.isEmpty()) {
            return mutableListOf()
        }

        val array = JSONArray(content)
        val items = mutableListOf<Member>()
        for (index in 0 until array.length()) {
            val obj = array.getJSONObject(index)
            items.add(Member.fromJson(obj))
        }
        return items
    }

    fun saveMembers(members: List<Member>) {
        val array = JSONArray()
        members.forEach { array.put(it.toJson()) }
        storageFile.writeText(array.toString())
    }
}
