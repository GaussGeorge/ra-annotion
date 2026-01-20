package rajomon

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SplitTokens 将请求中剩余的令牌（Budget）分配给下游服务。
// 场景：A 服务收到请求，扣除自身开销后还剩一些令牌。A 依赖 B 和 C 两个下游。
// 逻辑：
// 1. 先计算所有下游服务的总基础价格（入场费）。
// 2. 将剩余的“额外”令牌（盈余）平均分配给每个下游。
// 3. 每个下游最终获得的令牌 = 均分的盈余 + 该下游自身的基础价格。
// 返回一个切片，包含键值对字符串（下游服务名和对应的令牌数），用于注入 gRPC Metadata。
func (pt *PriceTable) SplitTokens(ctx context.Context, tokenleft int64, methodName string) ([]string, error) {
	downstreamNames := pt.callMap[methodName]
	size := len(downstreamNames)
	if size == 0 {
		return nil, nil // 没有下游，无需分配
	}

	downstreamTokens := []string{}
	// 获取该方法所有下游服务的价格总和（注意：这里取的是缓存的聚合价格，如果是 Additive 策略就是 Sum）
	downstreamPriceSum, _ := pt.RetrieveDSPrice(ctx, methodName)

	// 计算扣除基本价格后，每个下游服务能分到的“盈余”令牌
	// (总剩余 - 基础总价) / 下游数量
	tokenleftPerDownstream := (tokenleft - downstreamPriceSum) / int64(size)

	logger("[令牌分配]: 下游总价格为 %d, 共有 %d 个下游服务 (%s), 每个下游分配到的额外盈余为 %d\n",
		downstreamPriceSum, size, pt.nodeName, tokenleftPerDownstream)

	for _, downstreamName := range downstreamNames {
		// 读取特定下游节点的价格（键名格式：方法名-节点名）
		downstreamPriceString, _ := pt.priceTableMap.Load(methodName + "-" + downstreamName)
		downstreamPrice := downstreamPriceString.(int64)

		// 下游获得的最终令牌 = 均分的盈余 + 它自己的入场费(价格)
		downstreamToken := tokenleftPerDownstream + downstreamPrice

		// 构造 gRPC Metadata 键值对，例如 "tokens-serviceB", "50"
		downstreamTokens = append(downstreamTokens, "tokens-"+downstreamName, strconv.FormatInt(downstreamToken, 10))
		// logger("[Split tokens]:  token for %s is %d + %d\n", downstreamName, tokenleftPerDownstream, downstreamPrice)
	}
	return downstreamTokens, nil
}

// RetrieveDSPrice 获取指定方法的下游聚合价格
// 聚合价格可能是 Sum, Max 或 Mean，取决于配置
func (pt *PriceTable) RetrieveDSPrice(ctx context.Context, methodName string) (int64, error) {
	if len(pt.callMap[methodName]) == 0 {
		logger("[获取下游价格]:    %s 没有下游服务\n", methodName)
		return 0, nil
	}
	// 从价格表中加载该方法的下游价格（这个 key 存储的是聚合后的价格）
	downstreamPrice_string, ok := pt.priceTableMap.Load(methodName)
	if !ok || downstreamPrice_string == nil {
		return 0, errors.New("[获取下游价格] 未找到下游价格")
	}

	downstreamPrice, ok := downstreamPrice_string.(int64)
	if !ok {
		return 0, errors.New("[获取下游价格] 下游价格类型错误")
	}
	logger("[获取下游价格]:    %s 的下游价格为 %d\n", methodName, downstreamPrice)
	return downstreamPrice, nil
}

// RetrieveTotalPrice 计算总价格（自身价格 + 下游价格）
// 这是 Rajomon 的核心定价逻辑，根据配置的 priceAggregation 策略进行计算
func (pt *PriceTable) RetrieveTotalPrice(ctx context.Context, methodName string) (string, error) {
	ownPrice_string, _ := pt.priceTableMap.Load("ownprice")
	ownPrice := ownPrice_string.(int64)
	downstreamPrice, _ := pt.RetrieveDSPrice(ctx, methodName)

	var totalPrice int64
	if pt.priceAggregation == "maximal" {
		// [短板效应策略]: 总价格取 自身价格 和 下游价格 中的最大值
		// 这适用于并行调用场景，或者当瓶颈只在一个点时。最慢的环节决定整体延迟/成本。
		if ownPrice > downstreamPrice {
			totalPrice = ownPrice
		} else {
			totalPrice = downstreamPrice
		}
	} else if pt.priceAggregation == "additive" {
		// [累加策略]: 总价格 = 自身 + 下游
		// 适用于串行调用场景，每个环节的成本都要累加。
		totalPrice = ownPrice + downstreamPrice
	} else if pt.priceAggregation == "mean" {
		// [平均值策略]: 较少使用，用于平滑波动
		totalPrice = (ownPrice + downstreamPrice) / 2
	}

	price_string := strconv.FormatInt(totalPrice, 10)
	logger("[获取总价]: %s 的下游价格为 %d, 总计算价格为 %d \n", methodName, downstreamPrice, totalPrice)
	return price_string, nil
}

// UpdateOwnPrice 更新自身价格（简单策略：Step）
// 这种策略类似 TCP 拥塞控制中的 AIMD（加性增，乘性减）的简化版
func (pt *PriceTable) UpdateOwnPrice(congestion bool) error {

	ownPrice_string, _ := pt.priceTableMap.Load("ownprice")
	ownPrice := ownPrice_string.(int64)

	// 下面的日志代码已被移动到 decrementCounter() 用于 pinpointThroughput 模式
	logger("[更新自身价格]:  拥塞状态: %t, 当前价格: %d, 步长: %d\n", congestion, ownPrice, pt.priceStep)

	if congestion {
		// 如果检测到拥塞，价格直接上涨一个步长 (Step)
		ownPrice += pt.priceStep
		// 如果设置了指导价格（固定价格上限/目标），则直接锁定到指导价格，不再无限上涨
		if pt.guidePrice > -1 {
			ownPrice = pt.guidePrice
		}
	} else if ownPrice > 0 {
		// 如果不拥塞且价格大于0，缓慢降价（线性回退），模拟价格“冷却”
		ownPrice -= 1
	}
	pt.priceTableMap.Store("ownprice", ownPrice)
	logger("[更新自身价格]:  自身价格已更新为 %d\n", ownPrice)
	return nil
}

// UpdatePrice 更新自身价格（高级策略：Exponential Decay）
// 结合了线性调整和指数衰减，基于排队延迟的差异来动态计算步长
func (pt *PriceTable) UpdatePrice(ctx context.Context) error {
	// 1. 获取当前自身价格
	ownPriceInterface, _ := pt.priceTableMap.Load("ownprice")
	ownPrice := ownPriceInterface.(int64)

	var gapLatency float64
	// 2. 从 Context 中提取排队延迟 (GapLatency)
	val := ctx.Value(GapLatencyKey)
	if val == nil {
		gapLatency = 0.0
	} else {
		gapLatency = val.(float64)
	}

	// 3. 计算差异 (Error)：实际排队延迟 - 目标阈值
	// gapLatency 单位是 ms，需要 *1000 转微秒
	diff := int64(gapLatency*1000) - pt.latencyThreshold.Microseconds()

	// 4. 比例调整 (P-Controller)：根据延迟差异的大小来决定价格调整的幅度
	// 差异越大，涨价/降价越快
	adjustment := diff * pt.priceStep / 10000

	// 5. 实现指数衰减机制 (Exponential Decay)
	// 防止价格因为网络抖动而剧烈震荡 (Oscillation)
	if pt.priceStrategy == "expdecay" {
		if adjustment > 0 {
			// 如果连续多次涨价
			if pt.consecutiveIncreases >= 2 {
				// 且次数超过阈值，按 decayRate 进行衰减
				// 新调整幅度 = 原幅度 * (衰减率 ^ 连续次数)
				// 也就是涨得越来越慢
				adjustment = int64(float64(adjustment) * math.Pow(pt.decayRate, float64(pt.consecutiveIncreases)))
				logger("[价格步长衰减]: 价格步长减少比例 %f ** %d\n", pt.decayRate, pt.consecutiveIncreases)
			}
			pt.consecutiveIncreases++ // 增加连续上涨计数器
		} else if adjustment < 0 {
			// 如果价格开始下降，说明拥塞缓解，重置计数器，恢复灵敏度
			pt.consecutiveIncreases = 0
		}
	}

	logger("[基于排队延迟更新价格]: 自身价格 %d, 调整幅度 %d\n", ownPrice, adjustment)

	ownPrice += adjustment

	// 设置底价 (Reserve Price)：取 guidePrice 和 0 的较大值
	reservePrice := int64(math.Max(float64(pt.guidePrice), 0))

	// 确保价格不低于底价
	if ownPrice <= reservePrice {
		ownPrice = reservePrice
	}

	pt.priceTableMap.Store("ownprice", ownPrice)

	// 每 200 毫秒记录一次价格日志 (Sample Logging)
	if pt.lastUpdateTime.Add(200 * time.Millisecond).Before(time.Now()) {
		msgToLog := fmt.Sprintf("[Own price]: %d [Incremental Waiting Time Maximum]:    %.2f ms.\n", ownPrice, gapLatency)
		recordPrice(msgToLog)
		pt.lastUpdateTime = time.Now()
	}

	return nil
}

// UpdateDownstreamPrice 将下游返回的最新价格更新到本地价格表中
// 并根据聚合策略 (maximal/additive) 重新计算该方法的整体下游价格
func (pt *PriceTable) UpdateDownstreamPrice(ctx context.Context, method string, nodeName string, downstreamPrice int64) (int64, error) {

	// --- 策略 A: 取最大值 (Maximal) 或 平均值 (Mean) ---
	if pt.priceAggregation == "maximal" || pt.priceAggregation == "mean" {
		// 1. 更新特定下游节点的价格（Key: 方法名-节点名）
		pt.priceTableMap.Store(method+"-"+nodeName, downstreamPrice)
		logger("[收到响应]:    %s 的下游价格已更新为 %d\n", method+"-"+nodeName, downstreamPrice)

		// 2. 尝试优化更新：如果新价格比旧的聚合价格还高，直接更新聚合价格 (Fast Path)
		downstreamPrice_old, loaded := pt.priceTableMap.Load(method)
		if !loaded {
			// 如果之前没有加载过该方法的下游价格，记录日志并设为 0
			logger("[错误]:    无法找到 %s 之前的下游价格\n", method)
			downstreamPrice_old = int64(0)
		} else {
			logger("[之前的下游价格]:    更新前 %s 的下游价格为 %d.\n", method, downstreamPrice_old)
		}

		// 如果新收到的价格 > 旧的聚合最大值，那新的最大值肯定是它，无需遍历其他节点
		if downstreamPrice > downstreamPrice_old.(int64) {
			pt.priceTableMap.Store(method, downstreamPrice)
			logger("[更新下游价格]:    %s 的下游价格更新为 %d\n", method, downstreamPrice)
			return downstreamPrice, nil
		}

		// 3. 否则（新价格 <= 旧最大值）：
		// 可能是原本最贵的那个节点降价了，或者是便宜的节点涨价但没超过最大值。
		// 必须遍历所有相关的下游节点，重新寻找最大值 (Full Scan)。
		maxPrice := int64(math.MinInt64) // 初始化为最小整数

		pt.priceTableMap.Range(func(key, value interface{}) bool {
			// 查找所有以 "MethodName-" 开头的 Key
			if k, ok := key.(string); ok && strings.HasPrefix(k, method+"-") {
				if v, valid := value.(int64); valid && v > maxPrice {
					maxPrice = v
				}
			}
			return true
		})
		logger("[更新下游价格]: %s 的当前价格计算为 %d\n", method, maxPrice)
		// 仅更新当前请求涉及的方法的价格
		pt.priceTableMap.Store(method, maxPrice)

		// --- 策略 B: 累加 (Additive) ---
	} else if pt.priceAggregation == "additive" {
		// 1. 原子替换特定下游节点的价格，并获取旧值 (Swap)
		downstreamPrice_old, loaded := pt.priceTableMap.Swap(method+"-"+nodeName, downstreamPrice)
		if !loaded {
			return 0, status.Errorf(codes.Aborted, fmt.Sprintf("Downstream price of %s is not loaded", method+"-"+nodeName))
		}

		// 2. 计算差价 (Diff) = 新价格 - 旧价格
		// 这样可以避免全量遍历求和，只需要增量更新总价格
		diff := downstreamPrice - downstreamPrice_old.(int64)
		// 如果差价为 0，无需更新
		if diff == 0 {
			return 0, nil
		}

		// 3. 将差价传播给所有依赖该节点的方法
		// 遍历调用映射表
		for methodName, downstreamNames := range pt.callMap {
			for _, downstreamName := range downstreamNames {
				if downstreamName == nodeName {
					// 该方法依赖此节点，更新其总价格
					// 加载当前价格 -> 加上差价 -> 存回
					methodPrice, _ := pt.priceTableMap.Load(methodName)
					methodPrice = methodPrice.(int64) + diff
					pt.priceTableMap.Store(methodName, methodPrice)
					// 记录日志
					logger("[更新下游价格]: %s 的价格现在是 %d\n", methodName, methodPrice)
				}
			}
		}

		// 保存特定节点的新价格（用于日志记录或调试）
		pt.priceTableMap.Store(method+"-"+nodeName, downstreamPrice)
		logger("[收到响应]:    %s 的下游价格已更新为 %d\n", method+"-"+nodeName, downstreamPrice)
	}
	return downstreamPrice, nil
}
