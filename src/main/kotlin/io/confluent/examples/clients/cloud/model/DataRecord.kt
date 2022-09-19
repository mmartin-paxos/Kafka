package io.confluent.examples.clients.cloud.model

import com.google.gson.Gson

val gson = Gson()

data class DataRecord(var name: String = "") {
  override fun toString(): String {
    return gson.toJson(this)
  }
}
