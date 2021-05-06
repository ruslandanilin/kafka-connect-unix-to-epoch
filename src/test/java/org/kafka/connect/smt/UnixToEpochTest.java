/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
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

package org.kafka.connect.smt;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class UnixToEpochTest {

  private UnixToEpoch<SourceRecord> xform = new UnixToEpoch.Value<>();

  @After
  public void tearDown() throws Exception {
    xform.close();
  }

  @Test
  public void schemalessInsertUuidField() {
    final Map<String, Object> props = new HashMap<>();

    props.put("ts.field.name", "ts");

    xform.configure(props);

    final SourceRecord record = new SourceRecord(null, null, "test", 0,
      null, Collections.singletonMap("ts", 1620177595037L));

    final SourceRecord transformedRecord = xform.apply(record);
    assertEquals(1620177595, ((Map) transformedRecord.value()).get("ts"));
  }
}