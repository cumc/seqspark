/*
 * Copyright 2017 Zhang Di
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dizhang.seqspark.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.dizhang.seqspark.util.UserConfig.{MetaConfig, RootConfig}

/**
  * Created by zhangdi on 8/11/16.
  */

sealed trait SeqContext {
  def userConfig: UserConfig
  def sparkContext: SparkContext
}

case class SingleStudyContext(userConfig: RootConfig,
                              sparkContext: SparkContext,
                              sparkSession: SparkSession) extends SeqContext

case class MetaAnalysisContext(userConfig: RootConfig,
                               sparkContext: SparkContext) extends SeqContext


object SeqContext {
  def apply(cnf: RootConfig, sc: SparkContext, ss: SparkSession): SeqContext = {
    SingleStudyContext(cnf, sc, ss)
  }

  def apply(cnf: RootConfig, sc: SparkContext): SeqContext =
    MetaAnalysisContext(cnf, sc)
}
