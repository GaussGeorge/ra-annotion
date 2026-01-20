package rajomon

import (
	"context"
	"runtime/metrics"
	"sync/atomic"
	"time"
)

// 定义包级别的 context key 类型，防止与其他包的 context key 冲突
// 这是一个 Go 语言的最佳实践，避免使用 string 作为 key 导致冲突
type ctxKey string

// 定义具体的 key 常量
// GapLatency 指的是两个采样点之间的增量排队延迟
const GapLatencyKey ctxKey = "gapLatency"

// Increment 原子地增加吞吐量计数器
// 在高并发场景下，使用 atomic 操作比 mutex 锁性能更好
func (pt *PriceTable) Increment() {
	atomic.AddInt64(&pt.throughputCounter, 1)
}

// Decrement 原子地减少吞吐量计数器
func (pt *PriceTable) Decrement(step int64) {
	atomic.AddInt64(&pt.throughputCounter, -step)
}

// GetCount 获取当前的吞吐量计数，并将其重置为 0
// 注意：这是一个 "Read-and-Reset" 操作，用于获取上一个时间窗口内的总请求数
// 使用 atomic.SwapInt64 保证读取和重置是一个不可分割的原子操作
func (pt *PriceTable) GetCount() int64 {
	// 对应原子操作：取值并交换为0
	// return atomic.LoadInt64(&cc.throughtputCounter)
	return atomic.SwapInt64(&pt.throughputCounter, 0)
}

// latencyCheck 基于“观察到的延迟”进行周期性检查
// 这里的 latency 是业务逻辑中记录的实际处理耗时 (End-to-End Latency)
// 这是一个后台协程，会一直运行
func (pt *PriceTable) latencyCheck() {
	// time.Tick 创建一个定时器通道，每隔 priceUpdateRate 触发一次
	for range time.Tick(pt.priceUpdateRate) {
		// 创建一个新的 incoming context（此处原代码注释掉了 request-id 的生成）

		// create a new incoming context with the "request-id" as "0"
		// ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("request-id", "0"))

		// 更新自身价格：
		// 判定逻辑：如果 观察到的总延迟 > (延迟阈值 * 请求数量)
		// 这实际上是在比较：平均延迟(observedDelay / GetCount) 是否超过了 单次请求的延迟阈值(latencyThreshold)
		// pt.GetCount() 会重置计数器，pt.observedDelay 也会被重置，所以比较的是上一个时间窗口内的均值
		pt.UpdateOwnPrice(pt.observedDelay.Milliseconds() > pt.latencyThreshold.Milliseconds()*pt.GetCount())

		// 重置观察到的延迟累加值，准备下一个周期
		pt.observedDelay = time.Duration(0)
	}
}

// queuingCheck 检查 Go 协程(Goroutine)的排队延迟是否超过了 SLO (服务等级目标)
// 它利用 Go runtime/metrics 库来读取底层的调度器延迟直方图 (Scheduler Latency)
// 这是一个更底层的指标，反映了 CPU 饱和度，比业务延迟更早预警过载
func (pt *PriceTable) queuingCheck() {
	// 初始化一个空的直方图指针，用于存储上一次的快照
	var prevHist *metrics.Float64Histogram

	for range time.Tick(pt.priceUpdateRate) {
		// 开始计时，用于统计本次检查操作本身的开销（监控自身的性能损耗）
		start := time.Now()

		// 读取当前的运行时直方图 (Go Runtime Metrics)
		// 这个函数会调用 runtime/metrics.Read
		currHist := readHistogram()

		if prevHist == nil {
			// 如果是第一次运行，没有历史数据做对比，直接保存当前直方图并跳过本次循环
			prevHist = currHist
			continue
		}

		// 计算“间隙延迟”(Gap Latency)：
		// 即在上一个周期(prev)到当前周期(curr)之间，产生的排队延迟的最大值
		// 这是通过两个直方图相减（Differential Histogram）计算出来的
		gapLatency := maximumQueuingDelayms(prevHist, currHist)

		ctx := context.Background()

		logger("[增量等待时间最大值]: %f ms.\n", gapLatency)

		// 将计算出的排队延迟存入 context，传递给后续的 overloadDetection 函数使用
		ctx = context.WithValue(ctx, GapLatencyKey, gapLatency)

		// 根据价格策略更新价格
		if pt.priceStrategy == "step" {
			// step 策略：简单的步进调整 (涨/跌)
			// overloadDetection 返回 bool，true 表示过载需要涨价
			pt.UpdateOwnPrice(pt.overloadDetection(ctx))
		} else {
			// 其他策略（如 exponential）：可能直接根据 context 里的具体数值计算涨幅
			pt.UpdatePrice(ctx)
		}

		// 将当前直方图保存为“上一次”，用于下一次迭代的差分计算
		prevHist = currHist

		// 记录查询和计算直方图本身的耗时 (微秒转毫秒)
		logger("[查询延迟]:    计算开销为 %.2f 毫秒\n", float64(time.Since(start).Microseconds())/1000)
	}
}

// throughputCheck 仅基于吞吐量计数器进行检查
// 如果单位时间内的请求数 (RPS/QPS) 超过阈值，则认为过载
func (pt *PriceTable) throughputCheck() {
	for range time.Tick(pt.priceUpdateRate) {
		// 这里原先可能有直接减少计数器的逻辑，现已注释

		// pt.Decrement(pt.throughputThreshold)
		// ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("request-id", "0"))

		logger("[吞吐量计数器]:   当前计数为 %d\n", pt.throughputCounter)

		// 更新自身价格：
		// GetCount() 会返回当前周期的请求数并重置计数器
		// 如果 请求数 > 吞吐量阈值，则判定为过载，触发涨价
		pt.UpdateOwnPrice(pt.GetCount() > pt.throughputThreshold)
	}
}

// checkBoth 同时检查吞吐量和排队延迟 (And 逻辑)
// 这是一个更保守的策略：只有当两者都满足特定条件时，才触发价格调整
// 防止因为短时的毛刺 (Spike) 导致误判
func (pt *PriceTable) checkBoth() {
	var prevHist *metrics.Float64Histogram

	for range time.Tick(pt.priceUpdateRate) {
		logger("[吞吐量计数器]:   当前计数为 %d\n", pt.throughputCounter)

		// 获取当前直方图
		currHist := readHistogram()

		// 计算两个直方图之间的差异 (增量直方图)
		diff := metrics.Float64Histogram{}
		if prevHist == nil {
			// 如果没有历史数据，差异就是当前数据本身
			diff = *currHist
		} else {
			// 计算差值：curr - prev，得到这个时间窗口内的分布情况
			// 注意：GetHistogramDifference 和 readHistogram 一样，是辅助函数
			diff = GetHistogramDifference(*prevHist, *currHist)
		}

		// 从差异直方图中提取统计指标 (毫秒)
		gapLatency := maximumBucket(&diff)

		// 计算当前直方图（累计值）的中位数延迟
		cumulativeLat := medianBucket(currHist)

		logger("[累计等待时间中位数]:   %f ms.\n", cumulativeLat)
		logger("[增量等待时间 90分位]: %f ms.\n", percentileBucket(&diff, 90))
		logger("[增量等待时间中位数]:  %f ms.\n", medianBucket(&diff))
		logger("[增量等待时间最大值]:  %f ms.\n", maximumBucket(&diff))

		// 联合判定逻辑 (Overload Condition)：
		// 1. 吞吐量是否超过阈值 (pt.GetCount > pt.throughputThreshold)
		//    AND
		// 2. 增量最大排队延迟是否超过延迟阈值 (gapLatency > pt.latencyThreshold)
		// 注意：gapLatency 单位是 ms，latencyThreshold 是 Duration，所以做了单位转换比较
		pt.UpdateOwnPrice(pt.GetCount() > pt.throughputThreshold && int64(gapLatency*1000) > pt.latencyThreshold.Microseconds())

		// 更新历史直方图
		prevHist = currHist
	}
}

// overloadDetection 是一个辅助函数，用于从 Context 中提取信号并进行判定
// 输入信号：通常是 GapLatencyKey 对应的排队延迟
// 输出：bool (true 表示过载，false 表示正常)
func (pt *PriceTable) overloadDetection(ctx context.Context) bool {
	// 如果开启了基于排队延迟的检测 (pinpointQueuing)
	if pt.pinpointQueuing {
		var gapLatency float64

		// 从 context 中读取排队延迟数值
		val := ctx.Value(GapLatencyKey)
		if val == nil {
			gapLatency = 0.0
		} else {
			// 类型断言：将 interface{} 转回 float64
			gapLatency = val.(float64)
		}

		// 比较：如果 排队延迟 > 延迟阈值，则返回 true (过载)
		// gapLatency * 1000 将毫秒转换为微秒，与 Microseconds() 进行比较
		if int64(gapLatency*1000) > pt.latencyThreshold.Microseconds() {
			return true
		}
	}
	return false
}
