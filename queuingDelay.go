package rajomon

import (
	"fmt"
	"math"
	"runtime/metrics"
)

// medianBucket 计算直方图的中位数 (P50)
// 中位数表示：有一半的请求延迟低于这个数值。
func medianBucket(h *metrics.Float64Histogram) float64 {
	total := uint64(0)
	// 1. 计算总样本数（所有桶的计数之和）
	for _, count := range h.Counts {
		total += count
	}

	// 2. 计算中位数阈值（如果总是奇数则向上取整）
	// 比如总数是 100，阈值就是 50；总数是 101，阈值是 51
	thresh := (total + 1) / 2
	total = 0

	// 3. 寻找落入中位数的那个桶
	// 直方图的 Buckets 是边界值，Counts 是落在这个边界范围内的数量
	// 如果计数为0，通常跳过第一个桶（根据具体 buckets 定义，第一个通常是负无穷到最小边界）
	// 从索引 1 开始遍历桶
	for i := 1; i < len(h.Counts); i++ {
		total += h.Counts[i]
		// 一旦累积数量超过了阈值，说明中位数就在当前这个桶里
		if total >= thresh {
			// 找到累积计数超过阈值的桶，返回其上界
			// 乘以 1000 是将秒 (Seconds) 转换为毫秒 (Milliseconds)，更符合后端阅读习惯
			return h.Buckets[i] * 1000
		}
	}
	panic("should not happen") // 理论上不应运行到这里，除非直方图数据损坏
}

// percentileBucket 计算直方图的指定分位数 (例如 P99, P90)
// percentile: 输入 99 表示 P99，输入 90 表示 P90
func percentileBucket(h *metrics.Float64Histogram, percentile float64) float64 {
	total := uint64(0)
	// 计算样本总数
	for _, count := range h.Counts {
		total += count
	}

	// 计算分位数对应的目标阈值
	// 例如：总数 1000，求 P99，则阈值为 990。我们需要找到第 990 个数据落在哪个桶。
	thresh := uint64(math.Ceil(float64(total) * (percentile / 100.0)))
	total = 0

	// 遍历直方图计数，从第二个桶开始（索引1）
	// 寻找哪个桶使累积总数超过了阈值
	for i := 1; i < len(h.Counts); i++ {
		total += h.Counts[i]
		if total >= thresh {
			return h.Buckets[i] * 1000 // 转换为毫秒返回
		}
	}

	panic("should not happen")
}

// similarly, maximumBucket returns the maximum bucket
// maximumBucket 返回直方图中的最大值桶（即当前记录到的最大延迟）
// 这代表了系统中最慢的那一次请求或等待。
func maximumBucket(h *metrics.Float64Histogram) float64 {
	// 倒序遍历（从数值最大的桶开始往回找）
	// 这是一个优化：最大值肯定在后面，从后往前找能更快找到非空的桶
	for i := len(h.Counts) - 1; i >= 0; i-- {
		// 只要发现桶内计数不为0，说明有数据落在这个区间
		// 因为是从大到小找，找到的第一个非空桶就是最大值所在区间
		if h.Counts[i] != 0 {
			return h.Buckets[i] * 1000
		}
	}
	return 0
}

// GetHistogramDifference 计算两个 Float64Histogram 分布之间的差异（增量），并返回一个新的直方图。
// 它是通过将两个直方图对应的桶计数相减来实现的 (curr - prev)。
// 场景：Go 的监控指标通常是累计的（从启动到现在）。
// 如果我们要看“最近5秒内”的分布，就需要用“现在的快照”减去“5秒前的快照”。
func GetHistogramDifference(earlier, later metrics.Float64Histogram) metrics.Float64Histogram {
	// 如果之前的直方图计数为空（比如刚启动第一次采样），直接返回当前的
	if len(earlier.Counts) == 0 {
		return later
	}

	// 确保两个直方图具有相同数量的桶，否则无法对齐相减
	if len(earlier.Counts) != len(later.Counts) {
		panic("histograms have different number of buckets")
	}

	// 如果任何一个直方图为空，抛出 panic
	if len(earlier.Counts) == 0 || len(later.Counts) == 0 {
		panic("histogram has no buckets")
		// return &metrics.Float64Histogram{}
	}

	// Calculate the difference between the bucket counts and return the gap histogram
	// diff := metrics.Float64Histogram{}

	// 创建一个新的直方图对象来存储差异
	diff := metrics.Float64Histogram{
		Counts:  make([]uint64, len(earlier.Counts)),
		Buckets: earlier.Buckets, // 假设两个直方图的桶边界定义是相同的（通常 Go 版本不变就不会变）
	}

	// 核心逻辑：计算每个桶的增量 (later - earlier)
	for i := range earlier.Counts {
		diff.Counts[i] = later.Counts[i] - earlier.Counts[i]
	}
	return diff
}

// we should be able to avoid the GetHistogramDifference function by using the following function
// Find the maximum bucket between two Float64Histogram distributions
// maximumQueuingDelayms 是一个优化函数，避免了创建完整的差异直方图对象。
// 它直接比较两个快照，找出这期间发生的最大排队延迟。
// 这里的 Queuing Delay 指的是 Goroutine 等待被 CPU 调度执行的时间。
func maximumQueuingDelayms(earlier, later *metrics.Float64Histogram) float64 {
	// 倒序遍历 (从最大延迟的桶开始)
	for i := len(earlier.Counts) - 1; i >= 0; i-- {
		// 如果 later.Counts[i] > earlier.Counts[i]
		// 说明在两次采样的时间间隔内，有新的请求落入了第 i 个桶。
		// 因为我们是倒序查找，找到的第一个增长的桶，就是这段时间内出现过的最大延迟。
		if later.Counts[i] > earlier.Counts[i] {
			return later.Buckets[i] * 1000 // 转毫秒
		}
	}
	return 0
}

// readHistogram 从 runtime/metrics 读取当前的直方图数据
// 这是 Go 官方提供的底层监控接口，比 runtime.ReadMemStats 更强大。
func readHistogram() *metrics.Float64Histogram {
	// Create a sample for metric /sched/latencies:seconds and /sync/mutex/wait/total:seconds
	// 定义要读取的指标名称：Go 调度器延迟（单位：秒）
	// 这个指标衡量了 goroutine 从“准备好运行”到“实际开始运行”等待了多久。
	// 如果这个值很高，说明 CPU 忙不过来了（过载）。
	const queueingDelay = "/sched/latencies:seconds"
	measureMutexWait := false

	// Create a sample for the metric.
	// 创建采样对象切片
	sample := make([]metrics.Sample, 1)
	sample[0].Name = queueingDelay

	// 如果开启了锁等待监控（这里默认 false）
	if measureMutexWait {
		const mutexWait = "/sync/mutex/wait/total:seconds"
		sample[1].Name = mutexWait
	}

	// Sample the metric.
	// 执行采样读取，这是一个系统调用
	metrics.Read(sample)

	// Check if the metric is actually supported.
	// If it's not, the resulting value will always have
	// kind KindBad.
	// 检查指标是否被当前 Go 版本支持（兼容性检查）
	if sample[0].Value.Kind() == metrics.KindBad {
		panic(fmt.Sprintf("metric %q no longer supported", queueingDelay))
	}

	// get the current histogram
	// 获取 Float64Histogram 类型的数据结构
	currHist := sample[0].Value.Float64Histogram()

	return currHist
}

// // func printHistogram(h *metrics.Float64Histogram) prints the content of histogram h
// func printHistogram(h *metrics.Float64Histogram) {
//  // fmt.Printf("Histogram: %v\n", h)
//  fmt.Printf("Buckets: %v\n", h.Buckets)
//  fmt.Printf("Counts: %v\n", h.Counts)
// }
