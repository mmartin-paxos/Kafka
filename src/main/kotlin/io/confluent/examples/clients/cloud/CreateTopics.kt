/**
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
@file:JvmName("CreateTopics")

package io.confluent.examples.clients.cloud

import io.confluent.examples.clients.cloud.util.loadConfig
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.TopicExistsException
import java.util.Properties
import java.util.concurrent.ExecutionException

const val topic1 = "test1"
const val topic2 = "test2"
const val topic3 = "test3"


// Create topic in Confluent Cloud
fun createTopic(
    topic: String,
    partitions: Int,
    replication: Short,
    cloudConfig: Properties
) {
    val newTopic = NewTopic(topic, partitions, replication)


    try {
        println("Creating new topic $topic")
        with(AdminClient.create(cloudConfig)) {
            createTopics(listOf(newTopic)).all().get()
        }
    } catch (e: ExecutionException) {
        if (e.cause !is TopicExistsException) throw e
    }
}

fun main(args: Array<String>) {
    val props = loadConfig("java.config")
    // THIS ACTS AS AN EXTERNAL PRODUCER
    createTopic(topic1, 1, 1, props)
    createTopic(topic2, 1, 1, props)
    createTopic(topic3, 1, 1, props)


}


