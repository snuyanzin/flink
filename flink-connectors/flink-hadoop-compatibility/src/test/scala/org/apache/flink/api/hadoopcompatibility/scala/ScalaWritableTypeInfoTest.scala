/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.hadoopcompatibility.scala

import org.apache.flink.api.java.typeutils.WritableTypeInfo
import org.apache.flink.api.scala._

import org.apache.hadoop.io.Text
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ScalaWritableTypeInfoTest {

  @Test
  def testWritableTypeInfo = {
    val writableTypeInfo = createTypeInformation[Text]

    assertThat(writableTypeInfo).isInstanceOf(classOf[WritableTypeInfo[Text]])
    assertThat(classOf[Text]).isEqualTo(writableTypeInfo.getTypeClass)
  }
}
