package com.atguigu

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


//输入登录时间样例类
case class LoginEvent(useId: Long, ip: String, eventType: String, eventTime: Long)
//输出的异常报警信息样例类
case class Warning(useId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)
object loginfail_detect {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)

  //读取事件数据
  val resource = getClass.getResource("/LoginLog.csv")
  val loginEventStream = env.readTextFile(resource.getPath)
    .map( data => {
      val dataArray = data.split(",")
      LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
    })
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) { //设置延时时间为5s
      //设置时间戳
      override def extractTimestamp(element: LoginEvent) = element.eventTime * 1000L
    })

  val warningStream = loginEventStream
    .keyBy(_.useId)
    .process(new LoginWarning(2))

  warningStream.print()

  env.execute("login fail detect job")
}

class  LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning]{
 //定义状态，保存两秒内的所有登录失败的事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail"))


  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
//    val loginFailList = loginFailState.get()
//    // 判断类型是否是fail，只添加fail的信息到状态
//    if(value.eventType == "fail"){
//      if( !loginFailList.iterator().hasNext){
//        ctx.timerService().registerEventTimeTimer( value.eventTime * 1000L + 2000L)
//      }
//      loginFailState.add(value)
//    }else{
//      //如果是成功，清空状态
//      loginFailState.clear()
//    }

    if(value.eventType == "fail"){
      //如果是失败，判断之前是否有登录失败事件
      val iter = loginFailState.get().iterator()
      if(iter.hasNext){
        //如果有登录失败事件，就比较事件时间
        val firstFail = iter.next()
        if(value.eventTime < firstFail.eventTime + 2){
          //如果两次间隔小于两秒，输出报警
          out.collect(Warning(value.useId, firstFail.eventTime, value.eventTime, "login fail in 2 seconds"))
        }
        //更新最近一次的登录失败事件，保存在状态里
        loginFailState.clear()
        loginFailState.add(value)
      }else{
        //如果是第一次登录失败，直接添加到状态
        loginFailState.add(value)
      }
    }else{
      //如果成功，清空状态
      loginFailState.clear()
    }

  }

  //定时器触发时的操作  使用定时器存在的问题是两秒内最后一条数据为success，则会清空状态，如果前面数据在两秒内fail了两次将不会被检测到
//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
//    //触发定时器的时候，根据状态里的失败个数决定是否输出报警
//    val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
//    val iter = loginFailState.get().iterator()
//
//    while(iter.hasNext){
//      allLoginFails += iter.next()
//    }
//
//    if(allLoginFails.length >= 2){
//      out.collect(Warning(allLoginFails.head.useId, allLoginFails.head.eventTime, allLoginFails.last.eventTime, "login fail in 2 seconds for" + allLoginFails.length + "times"))
//    }
//
//    //如果是成功，清空状态
//    loginFailState.clear()
//  }
}
