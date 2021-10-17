package com.parth.scala

import com.parth.scala.mr1.configfile
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class configTest extends AnyFlatSpec with Matchers{
  behavior of "common configuration parameters"
  it should "obtain the time interval" in {configfile.getInt("common.time_interval") should be > 1}

  behavior of "mr1 parameters"
  it should "obtain temp path" in {configfile.getString("mr1.temp_path").length should be > 0}
  it should "obtain output path" in {configfile.getString("mr1.output_path").length should be > 0}

  behavior of "mr2 parameters"
  it should "obtain temp path" in {configfile.getString("mr2.temp_path").length should be > 0}
  it should "obtain output path" in {configfile.getString("mr2.output_path").length should be > 0}

  behavior of "mr3 parameters"
  it should "obtain output path" in {configfile.getString("mr3.output_path").length should be > 0}

  behavior of "mr4 parameters"
  it should "obtain output path" in {configfile.getString("mr4.output_path").length should be > 0}
}