package master

import (
   "github.com/mongodb/mongo-go-driver/mongo"
   "github.com/mongodb/mongo-go-driver/mongo/clientopt"
   "time"
   "context"
   "crontab/common"
   "github.com/mongodb/mongo-go-driver/mongo/findopt"
)

type LogMgr struct {
   client *mongo.Client
   logCollection *mongo.Collection
}

var(
   G_logMgr *LogMgr
)

//初始化
func InitLogMgr() (err error) {
   var(
	  client *mongo.Client
   )

   //建立mongodb链接
   if client,err=mongo.Connect(
	  context.TODO(),
	  G_config.MongodbUri,
	  clientopt.ConnectTimeout(time.Duration(G_config.MongodbConnectTimeout)*time.Millisecond)); err!=nil {
	  return
   }

   //赋值单例
   G_logMgr = &LogMgr{
      client:client,
      logCollection:client.Database("cron").Collection("log"),
   }
   return
}

//查询日志列表
func (logMgr *LogMgr) ListLog(name string,skip int,limit int) (logArr []*common.JobLog,err error) {
   var(
      filter *common.JobLogFilter
      logSort *common.SortLogByStartTime
      cursor mongo.Cursor
      jobLog *common.JobLog
   )

   //len(logArr)
   logArr = make([]*common.JobLog, 0)

   //过滤条件
   filter = &common.JobLogFilter{JobName:name}

   //按照任务开始时间倒排
   logSort = &common.SortLogByStartTime{-1}

   //查询
   if cursor,err = logMgr.logCollection.Find(context.TODO(),filter,findopt.Sort(logSort),findopt.Skip(int64(skip)),findopt.Limit(int64(limit))); err!=nil {
      return
   }
   //延迟释放游标
   defer cursor.Close(context.TODO())
	
   //遍历游标
   for cursor.Next(context.TODO()) {
      jobLog = &common.JobLog{}
      
      //反序列化BSON
	  if err = cursor.Decode(jobLog); err!=nil {
		 continue //有日志不合法
	  }

	  logArr = append(logArr,jobLog)
   }

   return
}