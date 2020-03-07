# UserBehaviorAnalysis
**电商用户行为数据分析**
#### requirements
* flink==1.7.2
* scala==2.11 
* kafka==2.2.0

#### 热门实时商品统计
* 基本需求

&ensp;&ensp;统计近1小时内的热门商品，每5分钟更新一次 

&ensp;&ensp;热门度用浏览次数（“pv”）来衡量 
* 解决思路

&ensp;&ensp;在所有用户行为数据中，过滤出浏览（“pv”）行为进行统计 

&ensp;&ensp;构建滑动窗口，窗口长度为1小时，滑动距离为5分钟 


#### 实时流量统计-热门页面
* 基本需求

&ensp;&ensp;从web服务器的日志中，统计实时的访问流量

&ensp;&ensp;统计每分钟的ip访问量，取出访问量最大的5个地址，每5秒更新一次
* 解决思路

&ensp;&ensp;将 apache 服务器日志中的时间，转换为时间戳，作为 Event Time

&ensp;&ensp;构建滑动窗口，窗口长度为1分钟，滑动距离为5秒
