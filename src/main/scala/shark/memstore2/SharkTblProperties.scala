/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.memstore2

import java.util.{Map => JavaMap}


/**
 * Collection of static fields and helpers for table properties (i.e., from A
 * CREATE TABLE TBLPROPERTIES( ... ) used by Shark.
 */
object SharkTblProperties {

  case class TableProperty(varname: String, defaultVal: String)

  // Default storage level for cached tables.
  val STORAGE_LEVEL = new TableProperty("shark.cache.storageLevel", "MEMORY_AND_DISK")

  // Class name of the default cache policy used to manage partition evictions for cached,
  // Hive-partitioned tables.
  val CACHE_POLICY = new TableProperty(
    "shark.cache.policy", "shark.memstore2.CacheAllPolicy")

  // Maximum size - in terms of the number of objects - of the cache specified by the
  // "shark.cache.partition.cachePolicy" property above.
  val MAX_PARTITION_CACHE_SIZE = new TableProperty("shark.cache.policy.maxSize", "10")

  // Default value for the "shark.cache.unify" table property.
  val UNIFY_VIEW_FLAG = new TableProperty("shark.cache.unifyView", "true")

  // Default value for the "shark.cache.reloadOnRestart" table property.
  val RELOAD_ON_RESTART_FLAG = new TableProperty("shark.cache.reloadOnRestart", "true")

  // Default value for the "shark.cache" table property
  val CACHE_FLAG = new TableProperty("shark.cache", "true")

  def getOrSetDefault(tblProps: JavaMap[String, String], variable: TableProperty): String = {
    if (!tblProps.containsKey(variable.varname)) {
      tblProps.put(variable.varname, variable.defaultVal)
    }
    tblProps.get(variable.varname)
  }

  /**
   * Returns value for the `variable` table property. If a value isn't present in `tblProps`, then
   * the default for `variable` will be returned.
   */
  def initializeWithDefaults(tblProps: JavaMap[String, String]): JavaMap[String, String] = {
    tblProps.put(CACHE_FLAG.varname, CACHE_FLAG.defaultVal)
    tblProps.put(UNIFY_VIEW_FLAG.varname, UNIFY_VIEW_FLAG.defaultVal)
    tblProps.put(STORAGE_LEVEL.varname, STORAGE_LEVEL.defaultVal)
    tblProps.put(RELOAD_ON_RESTART_FLAG.varname, RELOAD_ON_RESTART_FLAG.defaultVal)
    tblProps
  }

  def removeSharkProperties(tblProps: JavaMap[String, String], preserveRecoveryProps: Boolean) {
    tblProps.remove(CACHE_FLAG.varname)
    tblProps.remove(UNIFY_VIEW_FLAG.varname)
    if (!preserveRecoveryProps) {
      tblProps.remove(STORAGE_LEVEL.varname)
      tblProps.remove(RELOAD_ON_RESTART_FLAG.varname)
    }
  }
}
