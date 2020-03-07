package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


//定义输入数据的样例类
//case  class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )
case class UvCount( windowEnd: Long, uvCount: Long)

object UniqueVisitor {
  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //用相对路径定义数据源
    //    val resource = getClass.getResource("/UserBehavior.csv")
    //    val dataStream = env.readTextFile(resource.getPath)
    val dataStream = env.readTextFile("E:\\JAVA\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountByWindow())

    dataStream.print()

    env.execute()
  }
}

class UvCountByWindow()  extends  AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //定义一个Scala set, 用于保存所有的数据userId并去重
    var idSet = Set[Long]()
    //将当前窗口的所有数据的ID收集到set中，最后输出set的大小
    for(userBehavior <- input){
      idSet += userBehavior.userId
    }
    out.collect( UvCount( window.getEnd, idSet.size))
  }
}
