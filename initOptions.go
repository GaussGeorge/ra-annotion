package rajomon

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// NewRajomon 初始化 Rajomon 实例，配置价格表和相关参数
// Rajomon 是一个用于微服务过载控制和限流的系统
// nodeName: 当前节点的名称（如 "service-a", "client"）
// callmap: 定义服务间的调用关系（map[调用方方法名][]被调用方服务名）
// options: 各种配置参数，如是否开启限流、初始价格等
func NewRajomon(nodeName string, callmap map[string][]string, options map[string]interface{}) *PriceTable {
	// 初始化 PriceTable 结构体，设置默认值
	priceTable := &PriceTable{
		initprice:            0,                            // 初始价格
		nodeName:             nodeName,                     // 节点名称
		callMap:              callmap,                      // 调用关系图
		priceTableMap:        sync.Map{},                   // 线程安全的价格表，存储各服务的价格
		rateLimiting:         false,                        // 默认关闭限流
		rateLimitWaiting:     false,                        // 默认非阻塞限流（直接拒绝）
		loadShedding:         false,                        // 默认关闭负载削减
		pinpointThroughput:   false,                        // 默认关闭吞吐量检测
		pinpointLatency:      false,                        // 默认关闭延迟检测
		pinpointQueuing:      false,                        // 默认关闭排队检测
		rateLimiter:          make(chan int64, 1),          // 限流通道，容量为1，用于阻塞等待
		fakeInvoker:          false,                        // 默认真实调用
		skipPrice:            false,                        // 默认不跳过价格更新
		priceFreq:            5,                            // 价格反馈频率，每5次请求反馈一次
		tokensLeft:           10,                           // 初始令牌余额
		tokenUpdateRate:      time.Millisecond * 10,        // 令牌补充间隔 10ms
		lastUpdateTime:       time.Now(),                   // 上次更新时间
		lastRateLimitedTime:  time.Now().Add(-time.Second), // 上次被限流时间（初始化为过去）
		tokenUpdateStep:      1,                            // 每次补充1个令牌
		tokenRefillDist:      "fixed",                      // 令牌填充模式：固定速率
		tokenStrategy:        "all",                        // 令牌分配策略
		priceStrategy:        "step",                       // 价格调整策略：步进式
		throughputCounter:    0,                            // 吞吐量计数器
		priceUpdateRate:      time.Millisecond * 10,        // 价格更新间隔 10ms
		observedDelay:        time.Duration(0),             // 观测到的延迟
		clientTimeOut:        time.Duration(0),             // 客户端超时时间
		clientBackoff:        time.Duration(0),             // 客户端退避时间
		randomRateLimit:      -1,                           // 随机限流阈值（-1表示关闭）
		throughputThreshold:  0,                            // 吞吐量阈值
		latencyThreshold:     time.Duration(0),             // 延迟阈值
		priceStep:            1,                            // 价格调整步长
		priceAggregation:     "maximal",                    // 价格聚合策略：取最大值
		guidePrice:           -1,                           // 指导价格（-1表示未设置）
		consecutiveIncreases: 0,                            // 连续涨价次数计数
		decayRate:            0.8,                          // 衰减率
		maxToken:             10,                           // 最大令牌数（桶容量）
	}

	// 创建一个新的传入上下文，将 "request-id" 设置为 "0" (注释代码，暂未使用)
	// ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("request-id", "0"))

	// --- 解析 options map 中的配置选项 ---
	// 使用类型断言 .(type) 安全地获取配置值

	if debugOpt, ok := options["debug"].(bool); ok {
		debug = debugOpt // 设置全局 debug 标志
	}

	if trackingPrice, ok := options["recordPrice"].(bool); ok {
		trackPrice = trackingPrice // 设置是否记录价格历史
	}

	// 设置初始价格
	if initprice, ok := options["initprice"].(int64); ok {
		priceTable.initprice = initprice
		// 如果不是客户端节点，打印日志确认
		logger("initprice of %s set to %d\n", nodeName, priceTable.initprice)
	}

	// 开启速率限制功能 (Rate Limiting)
	// 通常在客户端侧开启，控制发送速率
	if rateLimiting, ok := options["rateLimiting"].(bool); ok {
		priceTable.rateLimiting = rateLimiting
		logger("rateLimiting        of %s set to %v\n", nodeName, rateLimiting)
	}

	// 开启负载削减 (Load Shedding) 功能
	// 通常在服务端侧开启，当过载时主动拒绝请求
	if loadShedding, ok := options["loadShedding"].(bool); ok {
		priceTable.loadShedding = loadShedding
		logger("loadShedding        of %s set to %v\n", nodeName, loadShedding)
	}

	// 开启基于吞吐量的过载检测 (Throughput-based Detection)
	if pinpointThroughput, ok := options["pinpointThroughput"].(bool); ok {
		priceTable.pinpointThroughput = pinpointThroughput
		logger("pinpointThroughput  of %s set to %v\n", nodeName, pinpointThroughput)
	}

	// 开启基于延迟的过载检测 (Latency-based Detection)
	if pinpointLatency, ok := options["pinpointLatency"].(bool); ok {
		priceTable.pinpointLatency = pinpointLatency
		logger("pinpointLatency     of %s set to %v\n", nodeName, pinpointLatency)
	}

	// 开启基于排队时间的过载检测 (Queuing-based Detection)
	// 这通常比单纯的延迟检测更准确反应拥塞情况
	if pinpointQueuing, ok := options["pinpointQueuing"].(bool); ok {
		priceTable.pinpointQueuing = pinpointQueuing
		logger("pinpointQueuing     of %s set to %v\n", nodeName, pinpointQueuing)
	}

	// 开启伪调用模式 (Fake Invoker)
	// 用于压测控制平面的开销，不执行实际业务逻辑，减少干扰
	if fakeInvoker, ok := options["fakeInvoker"].(bool); ok {
		priceTable.fakeInvoker = fakeInvoker
		logger("fakeInvoker     of %s set to %v\n", nodeName, fakeInvoker)
	}

	// 开启懒响应模式 (Lazy Response)
	// 只有在特定条件下才在响应中携带价格信息，减少网络开销
	if skipPrice, ok := options["lazyResponse"].(bool); ok {
		priceTable.skipPrice = skipPrice
		logger("skipPrice       of %s set to %v\n", nodeName, skipPrice)
	}

	// 设置价格更新频率 (Price Frequency)
	// 例如每 N 个请求更新一次价格，用于平滑波动
	if priceFreq, ok := options["priceFreq"].(int64); ok {
		priceTable.priceFreq = priceFreq
		logger("priceFreq       of %s set to %v\n", nodeName, priceFreq)
	}

	// 设置初始剩余令牌数 (Initial Tokens)
	if tokensLeft, ok := options["tokensLeft"].(int64); ok {
		priceTable.tokensLeft = tokensLeft
		logger("tokensLeft      of %s set to %v\n", nodeName, tokensLeft)
	}

	// 设置令牌更新速率 (Token Refill Rate)
	// 多久补充一次令牌
	if tokenUpdateRate, ok := options["tokenUpdateRate"].(time.Duration); ok {
		priceTable.tokenUpdateRate = tokenUpdateRate
		logger("tokenUpdateRate     of %s set to %v\n", nodeName, tokenUpdateRate)
	}

	// 设置每次更新增加的令牌数量 (Token Refill Step)
	if tokenUpdateStep, ok := options["tokenUpdateStep"].(int64); ok {
		priceTable.tokenUpdateStep = tokenUpdateStep
		logger("tokenUpdateStep     of %s set to %v\n", nodeName, tokenUpdateStep)
	}

	// 设置令牌填充的分布模式 (Token Refill Distribution)
	// fixed: 固定速率填充
	// uniform: 均匀随机填充
	// poisson: 泊松过程填充 (更接近真实流量特征)
	if tokenRefillDist, ok := options["tokenRefillDist"].(string); ok {
		// 如果分布模式不是 "fixed"、"uniform" 或 "poisson"，则默认为 "fixed"
		if tokenRefillDist != "fixed" && tokenRefillDist != "uniform" && tokenRefillDist != "poisson" {
			tokenRefillDist = "fixed"
		}
		priceTable.tokenRefillDist = tokenRefillDist
		logger("tokenRefillDist     of %s set to %v\n", nodeName, tokenRefillDist)
	}

	// 设置令牌策略 (Token Strategy)
	// all: 一次性分发
	// uniform: 均匀分发
	if tokenStrategy, ok := options["tokenStrategy"].(string); ok {
		// 校验策略合法性，默认为 "all"
		if tokenStrategy != "all" && tokenStrategy != "uniform" {
			tokenStrategy = "all"
		}
		priceTable.tokenStrategy = tokenStrategy
		logger("tokenStrategy       of %s set to %v\n", nodeName, tokenStrategy)
	}

	// 设置价格调整策略 (Price Strategy)
	// step: 步进式调整
	// exponential: 指数级调整
	if priceStrategy, ok := options["priceStrategy"].(string); ok {
		priceTable.priceStrategy = priceStrategy
		logger("priceStrategy       of %s set to %v\n", nodeName, priceStrategy)
	}

	// 设置价格更新速率 (Price Update Rate)
	if priceUpdateRate, ok := options["priceUpdateRate"].(time.Duration); ok {
		priceTable.priceUpdateRate = priceUpdateRate
		logger("priceUpdateRate     of %s set to %v\n", nodeName, priceUpdateRate)
	}

	// 设置客户端超时时间 (Client Timeout)
	// 客户端等待令牌的最大时长
	if clientTimeOut, ok := options["clientTimeOut"].(time.Duration); ok {
		priceTable.clientTimeOut = clientTimeOut
		logger("clientTimeout       of %s set to %v\n", nodeName, clientTimeOut)
	}

	// 设置客户端退避时间 (Client Backoff)
	// 被限流后等待多久再重试
	if clientBackoff, ok := options["clientBackoff"].(time.Duration); ok {
		priceTable.clientBackoff = clientBackoff
		logger("clientBackoff       of %s set to %v\n", nodeName, clientBackoff)
	}

	// 设置随机限流阈值 (Random Rate Limit)
	// 用于测试目的，随机丢弃请求
	if randomRateLimit, ok := options["randomRateLimit"].(int64); ok {
		priceTable.randomRateLimit = randomRateLimit
		logger("randomRateLimit     of %s set to %v\n", nodeName, randomRateLimit)
	}

	// 决定限流行为：等待 vs 直接拒绝
	// 当设置了客户端超时时间（>0）时，启用限流等待模式 (Blocking Mode)
	// 否则为非阻塞模式 (Non-blocking Mode)，直接返回错误
	if priceTable.clientTimeOut > 0 {
		priceTable.rateLimitWaiting = true
	} else {
		priceTable.rateLimitWaiting = false
	}
	logger("rateLimitWaiting    of %s set to %v\n", nodeName, priceTable.rateLimitWaiting)

	// 设置吞吐量阈值 (Throughput Threshold)
	if throughputThreshold, ok := options["throughputThreshold"].(int64); ok {
		priceTable.throughputThreshold = throughputThreshold
		logger("throughputThreshold of %s set to %v\n", nodeName, throughputThreshold)
	}

	// 设置延迟阈值 (Latency Threshold)
	if latencyThreshold, ok := options["latencyThreshold"].(time.Duration); ok {
		priceTable.latencyThreshold = latencyThreshold
		logger("latencyThreshold    of %s set to %v\n", nodeName, latencyThreshold)
	}

	// 设置价格调整步长 (Price Step)
	// 每次涨价或降价的幅度
	if priceStep, ok := options["priceStep"].(int64); ok {
		priceTable.priceStep = priceStep
		logger("priceStep       of %s set to %v\n", nodeName, priceStep)
	}

	// 设置价格聚合方式 (Price Aggregation)
	// 决定如何计算总价格（自身价格 + 下游价格）
	// maximal: 取最大值（适用于并发调用，取决于最慢的路径）
	// additive: 累加（适用于串行调用，成本累加）
	// mean: 平均值
	if priceAggregation, ok := options["priceAggregation"].(string); ok {
		if priceAggregation != "maximal" && priceAggregation != "additive" && priceAggregation != "mean" {
			priceAggregation = "maximal"
		}
		priceTable.priceAggregation = priceAggregation
		logger("priceAggregation    of %s set to %v\n", nodeName, priceAggregation)
	}

	// 设置指导价格 (Guide Price)
	// 作为一个参考基准价格
	if guidePrice, ok := options["guidePrice"].(int64); ok {
		priceTable.guidePrice = guidePrice
		logger("guidePrice      of %s set to %v\n", nodeName, guidePrice)
	}

	// --- 启动后台任务 (Background Tasks) ---

	if priceTable.nodeName == "client" {
		// 如果是客户端节点，启动“发工资”协程 (tokenRefill)
		// 定期向桶里补充令牌
		go priceTable.tokenRefill(priceTable.tokenRefillDist, priceTable.tokenUpdateStep, priceTable.tokenUpdateRate)
	} else {
		// 如果是服务端节点，启动“监控”协程
		// 根据配置开启相应的过载检测逻辑
		if priceTable.pinpointQueuing && priceTable.pinpointThroughput {
			go priceTable.checkBoth() // 同时检测排队时间和吞吐量
		} else if priceTable.pinpointThroughput {
			go priceTable.throughputCheck() // 仅检测吞吐量
		} else if priceTable.pinpointLatency {
			go priceTable.latencyCheck() // 仅检测延迟
		} else if priceTable.pinpointQueuing {
			go priceTable.queuingCheck() // 仅检测排队时间
		}
	}

	// --- 初始化价格表 (Init Price Table) ---

	// 将初始价格存入 priceTableMap，键为 "ownprice" (自身价格)
	priceTable.priceTableMap.Store("ownprice", priceTable.initprice)

	// 遍历调用映射，初始化所有相关下游方法的价格
	for method, nodes := range priceTable.callMap {
		// 存储该方法的默认价格
		priceTable.priceTableMap.Store(method, priceTable.initprice)
		logger("[InitPriceTable]: Method %s price set to %d\n", method, priceTable.initprice)

		// 存储该方法在特定下游节点上的价格 (method-nodeName 组合键)
		// 这允许针对不同下游实例维护不同的价格
		for _, node := range nodes {
			priceTable.priceTableMap.Store(method+"-"+node, priceTable.initprice)
			logger("[InitPriceTable]: Method %s-%s price set to %d\n", method, node, priceTable.initprice)
		}
	}
	return priceTable
}

// GetTokensLeft 原子操作读取剩余令牌数 tokensLeft
// 保证并发安全
func (pt *PriceTable) GetTokensLeft() int64 {
	if !atomicTokens {
		// 如果未开启原子操作模式 (atomicTokens=false)，直接读取
		// 注意：这种模式在并发下可能不安全，通常用于非并发场景或性能测试
		return pt.tokensLeft
	}
	// 原子加载，确保读到最新的值
	return atomic.LoadInt64(&pt.tokensLeft)
}

// DeductTokens 原子操作扣除令牌
// 返回是否扣除成功
func (pt *PriceTable) DeductTokens(n int64) bool {
	if !atomicTokens {
		if pt.tokensLeft-n < 0 {
			return false // 令牌不足，扣除失败
		} else {
			pt.tokensLeft -= n
			return true
		}
	}
	// 使用 CAS (Compare-And-Swap) 循环确保并发安全
	// 类似于乐观锁机制
	for {
		currentTokens := pt.GetTokensLeft()
		newTokens := currentTokens - n
		if newTokens < 0 {
			// 令牌不足，扣除失败，不再重试
			return false
		}
		// 尝试更新：如果内存中的值还是 currentTokens，则更新为 newTokens
		// 如果更新成功，返回 true；如果失败（说明被别的协程改过了），循环重试
		if atomic.CompareAndSwapInt64(&pt.tokensLeft, currentTokens, newTokens) {
			return true
		}
	}
}

// AddTokens 原子操作增加令牌
// 只需要增加，不需要像扣除那样检查是否小于0，所以 atomic.AddInt64 即可
func (pt *PriceTable) AddTokens(n int64) {
	if !atomicTokens {
		pt.tokensLeft += n
		return
	}
	atomic.AddInt64(&pt.tokensLeft, n)
}

// tokenRefill 是一个后台协程，用于向价格表的令牌桶中填充令牌
// 模拟“发工资”或令牌桶算法中的令牌生成
func (pt *PriceTable) tokenRefill(tokenRefillDist string, tokenUpdateStep int64, tokenUpdateRate time.Duration) {
	if tokenRefillDist == "poisson" {
		// --- 泊松分布模式 ---
		// 模拟更真实的随机流量到达模式

		// 创建一个 ticker，使用初始 tick 间隔
		ticker := time.NewTicker(pt.initialTokenUpdateInterval())
		defer ticker.Stop()

		// 计算 lambda (速率参数)，即 1 除以 tokenUpdateRate（毫秒），并转换为 float64
		// lambda 表示每毫秒期望发生的事件数
		lambda := float64(1) / float64(tokenUpdateRate.Milliseconds())

		for range ticker.C {
			// 确定性地给客户端增加令牌
			pt.AddTokens(tokenUpdateStep)

			if pt.rateLimitWaiting {
				// 如果有限流等待机制，尝试唤醒一个等待的请求
				// pt.lastUpdateTime = time.Now()
				pt.unblockRateLimiter() // 如果有等待的请求，尝试解锁
			}

			// 基于指数分布调整下一次 tick 的间隔，模拟泊松过程的时间间隔特性
			// 每次循环间隔都不一样
			ticker.Reset(pt.nextTokenUpdateInterval(lambda))
		}
	} else {
		// --- 固定或均匀分布模式 ---
		// 使用 time.Tick 创建一个固定周期的通道
		for range time.Tick(tokenUpdateRate) {
			// 根据 tokenRefillDist 确定性或随机地增加令牌
			if tokenRefillDist == "fixed" {
				// 固定模式：每次增加固定数量
				pt.AddTokens(tokenUpdateStep)
			} else if tokenRefillDist == "uniform" {
				// 均匀分布：增加 0 到 2*step 之间的随机数
				// 平均值仍然是 tokenUpdateStep
				pt.AddTokens(rand.Int63n(tokenUpdateStep * 2))
			}
			if pt.rateLimitWaiting {
				// pt.lastUpdateTime = time.Now()
				pt.unblockRateLimiter()
			}
		}
	}
}

// initialTokenUpdateInterval 返回 tokenRefill 的初始 tick 间隔
func (pt *PriceTable) initialTokenUpdateInterval() time.Duration {
	// 返回预设的初始间隔
	return pt.tokenUpdateRate
}

// nextTokenUpdateInterval 基于指数分布计算下一次 tokenRefill 的 tick 间隔
// 用于实现泊松分布的令牌填充
func (pt *PriceTable) nextTokenUpdateInterval(lambda float64) time.Duration {
	// 使用指数分布计算间隔时间 (Exponential Distribution)
	// 泊松过程的事件间隔服从指数分布
	// rand.ExpFloat64() 返回一个服从标准指数分布的随机数
	// 除以 lambda 得到均值为 1/lambda 的随机数
	nextTickDuration := time.Duration(rand.ExpFloat64()/lambda) * time.Millisecond

	if nextTickDuration <= 0 {
		// 处理计算结果为非正数的情况，防止定时器报错
		nextTickDuration = time.Millisecond // 设置一个默认的最小正间隔
	}
	// 返回下一次 tick 的持续时间
	return time.Duration(nextTickDuration)
}
