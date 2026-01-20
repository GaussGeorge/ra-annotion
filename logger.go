package rajomon

import (
	"fmt"
	"time"
)

// logger 用于模拟一个完善的日志系统。
// 为了简化示例，这里仅仅是将内容打印到控制台。
// node: 在实际生产级的后端项目中，通常会使用 zap, logrus 或 zerolog 等结构化日志库，而不是 fmt.Printf。
// format: 格式化字符串，例如 "Error: %s"
// a ...interface{}: Go 语言的可变参数语法。表示函数可以接受任意数量、任意类型的参数。
func logger(format string, a ...interface{}) {
	// 仅在开启调试模式 (debug 为 true) 时输出日志
	// 注意：debug 变量通常在包级别定义或由配置初始化 (在 rajomon.go 中定义)
	if debug {
		// 获取当前时间，格式化为 RFC3339Nano 格式（精确到纳秒）
		// Go 语言的时间格式化非常独特，必须使用固定的参考时间：Mon Jan 2 15:04:05 MST 2006
		// 这里的 "2006-01-02..." 就是在这个参考时间基础上修改格式
		timestamp := time.Now().Format("2006-01-02T15:04:05.999999999-07:00")

		// 打印格式：LOG: 时间戳 | 内容
		// 使用 fmt.Printf 支持格式化字符串（如 %s, %d 等）
		// a... 表示将切片 a 打散作为多个参数传入 Printf
		fmt.Printf("LOG: "+timestamp+"|\t"+format+"\n", a...)
	}
}

// recordPrice 用于记录价格追踪日志，通常用于后续分析价格波动
// 这是一个业务层面的埋点（Metrics），不同于上面的系统调试日志。
func recordPrice(format string, a ...interface{}) {
	// 仅在开启价格追踪 (trackPrice 为 true) 时输出
	// trackPrice 也是在包级别定义的全局变量
	if trackPrice {
		// 同样获取纳秒级时间戳，对于价格这种高频变动的数据，时间精度很重要
		timestamp := time.Now().Format("2006-01-02T15:04:05.999999999-07:00")

		// 打印日志，通常这部分输出会被收集到 ELK 或 Prometheus 等监控系统中
		fmt.Printf("LOG: "+timestamp+"|\t"+format+"\n", a...)
	}
}
