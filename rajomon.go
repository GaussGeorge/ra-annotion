package rajomon

import (
	"context"
	"errors"
	"fmt"
	"log"

	"strconv"
	"sync"
	"time"

	"github.com/bytedance/gopkg/lang/fastrand"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	// InsufficientTokens 当请求数量超过限流器容量（即令牌不足以支付价格）时返回此错误。
	// [后端概念]: 哨兵错误 (Sentinel Errors)，用于在逻辑流中进行特定的错误判断
	InsufficientTokens = errors.New("接收到的令牌不足，触发负载削减 (Load Shedding)。")
	// RateLimited 当发送端没有足够的令牌发送请求时返回此错误。
	RateLimited = errors.New("令牌不足无法发送，触发限流 (Rate Limit)。")
	// gRPC 标准错误：参数非法
	errMissingMetadata = status.Errorf(codes.InvalidArgument, "缺少元数据 (metadata)")
	debug              = false
	atomicTokens       = false
	trackPrice         = false
)

// PriceTable 实现了 Rajomon 的核心价格表结构
type PriceTable struct {
	// 下面的无锁哈希表包含总价格、自身价格和下游价格
	// initprice 是价格表的初始价格
	initprice int64
	nodeName  string              // 当前节点名称
	callMap   map[string][]string // 调用关系图：方法名 -> 下游服务列表

	// [Go概念]: sync.Map 是 Go 标准库提供的并发安全的 Map
	// 在读多写少的场景下性能优于 map + RWMutex
	priceTableMap sync.Map

	rateLimiting       bool // 开关：是否开启客户端限流
	rateLimitWaiting   bool // 开关：限流时是否等待令牌（阻塞模式）
	loadShedding       bool // 开关：是否开启服务端负载削减
	pinpointThroughput bool // 开关：是否基于吞吐量检测过载
	pinpointLatency    bool // 开关：是否基于延迟检测过载
	pinpointQueuing    bool // 开关：是否基于排队时间检测过载 (关键特性)

	// [Go概念]: Channel (通道)
	// 这里用作信号量或阻塞队列。当令牌不足时，请求会在这个通道上等待。
	rateLimiter chan int64

	fakeInvoker          bool          // 开关：伪调用模式（不实际发送请求，用于压测开销）
	skipPrice            bool          // 开关：是否跳过价格更新（懒响应）
	priceFreq            int64         // 价格反馈频率（例如每 n 个请求反馈一次）
	tokensLeft           int64         // 剩余令牌数（钱包余额）
	tokenUpdateRate      time.Duration // 令牌更新速率
	lastUpdateTime       time.Time     // 上次更新时间
	lastRateLimitedTime  time.Time     // 上次被限流的时间
	tokenUpdateStep      int64         // 每次更新增加的令牌数
	tokenRefillDist      string        // 令牌填充分布 (fixed, uniform, poisson)
	tokenStrategy        string        // 令牌分配策略 (all, uniform)
	priceStrategy        string        // 价格调整策略 (step, expdecay)
	throughputCounter    int64         // 吞吐量计数器
	priceUpdateRate      time.Duration // 价格更新速率
	observedDelay        time.Duration // 观测到的业务延迟
	clientTimeOut        time.Duration // 客户端等待令牌的超时时间
	clientBackoff        time.Duration // 客户端退避时间
	randomRateLimit      int64         // 随机限流阈值 (用于调试)
	throughputThreshold  int64         // 吞吐量阈值
	latencyThreshold     time.Duration // 延迟阈值
	priceStep            int64         // 价格调整步长
	priceAggregation     string        // 价格聚合策略 (maximal, additive, mean)
	guidePrice           int64         // 指导价格 (目标价格)
	consecutiveIncreases int64         // 价格连续上涨次数 (用于指数衰减策略)
	decayRate            float64       // 衰减率
	maxToken             int64         // 最大令牌容量
}

/*
UnblockRateLimiter 解锁限流器通道。
非阻塞发送，如果通道已满则直接返回。
*/
func (pt *PriceTable) unblockRateLimiter() {
	// [Go概念]: select + default 实现非阻塞发送
	// 尝试向 rateLimiter 发送信号，如果通道已满（没有人在等待），则走 default 分支直接返回，不会卡死。
	select {
	case pt.rateLimiter <- 1:
		return
	default:
		return
	}
}

// RateLimiting 供终端用户（人类客户端）使用。
// 检查当前令牌是否足以支付目标服务的价格。如果 tokens < prices，则阻止调用。
func (pt *PriceTable) RateLimiting(ctx context.Context, tokens int64, methodName string) error {
	// RetrieveDSPrice 获取下游服务的当前价格（在 tokenAndPrice.go 中定义）
	servicePrice, _ := pt.RetrieveDSPrice(ctx, methodName)
	extratoken := tokens - servicePrice
	logger("[限流检查]: 正在检查请求。持有令牌 %d, 方法 %s 的价格是 %d\n", tokens, methodName, servicePrice)

	// 预算不足，返回限流错误
	if extratoken < 0 {
		logger("[准备请求]: 因令牌不足请求被阻塞。")
		return RateLimited
	}
	return nil
}

// LoadShedding 服务端核心准入控制逻辑。
// 根据价格表从请求中扣除令牌，然后根据请求情况更新价格表。
// 返回值：
// 1. 扣除自身价格后剩余的令牌数 (用于 additive 策略传给下游)
// 2. 当前的总价格字符串 (用于返回给客户端)
// 3. 错误信息 (如果令牌不足则返回 InsufficientTokens)
func (pt *PriceTable) LoadShedding(ctx context.Context, tokens int64, methodName string) (int64, string, error) {
	// [后端概念]: 负载削减 (Load Shedding)
	// 当服务器发现自己处理不过来（价格高）且客户端带来的“钱”（Token）不够时，主动拒绝请求。
	// 这比让请求排队超时要好，能保护服务器不被打挂。

	// 如果未开启负载削减，直接放行，返回原令牌数
	if !pt.loadShedding {
		totalPrice, _ := pt.RetrieveTotalPrice(ctx, methodName)
		return tokens, totalPrice, nil
	}

	ownPrice_string, ok := pt.priceTableMap.Load("ownprice")
	if !ok {
		return 0, "", status.Errorf(codes.Internal, "[负载削减]: 未找到 %s 的自身价格", methodName)
	}
	ownPrice := ownPrice_string.(int64)
	downstreamPrice, err := pt.RetrieveDSPrice(ctx, methodName)
	if err != nil {
		logger("[负载削减]: 获取 %s 的下游价格失败，错误: %v\n", methodName, err)
		return 0, "", err
	}

	// --- 策略 A: 最大值聚合 (Maximal) ---
	// 适用于并行调用场景，总价格取决于链路中最贵的（最慢的）服务（短板效应）
	if pt.priceAggregation == "maximal" {

		logger("[收到请求]: 方法 %s, 自身价格 %d, 下游价格 %d。\n", methodName, ownPrice, downstreamPrice)

		// 取自身价格和下游价格的最大值作为门槛
		if ownPrice < downstreamPrice {
			ownPrice = downstreamPrice
		}

		// 准入判断：带来的令牌是否大于当前价格
		if tokens >= ownPrice {
			logger("[执行 AQM]: 请求被接受。持有令牌 %d, 当前价格 %d\n", tokens, ownPrice)
			return tokens - ownPrice, strconv.FormatInt(ownPrice, 10), nil
		} else {
			logger("[执行 AQM]: 请求因令牌不足被拒绝。持有令牌 %d, 当前价格 %d\n", tokens, ownPrice)
			return 0, strconv.FormatInt(ownPrice, 10), InsufficientTokens
		}

		// --- 策略 B: 平均值聚合 (Mean) ---
	} else if pt.priceAggregation == "mean" {
		// 使用自身价格和下游价格的平均值作为最终价格
		totalPrice := (ownPrice + downstreamPrice) / 2
		logger("[收到请求]: 总价格 %d, 自身价格 %d, 下游价格 %d\n", totalPrice, ownPrice, downstreamPrice)
		if tokens >= totalPrice {
			logger("[收到请求]: 请求被接受。持有令牌 %d, 当前价格 %d\n", tokens, totalPrice)
			return tokens - totalPrice, strconv.FormatInt(totalPrice, 10), nil
		} else {
			logger("[收到请求]: 请求因令牌不足被拒绝。持有令牌 %d, 当前价格 %d\n", tokens, totalPrice)
			return 0, strconv.FormatInt(totalPrice, 10), InsufficientTokens
		}

		// --- 策略 C: 累加聚合 (Additive) ---
		// 适用于串行调用场景，总价格 = 自身价格 + 下游价格
	} else if pt.priceAggregation == "additive" {
		totalPrice := ownPrice + downstreamPrice

		extratoken := tokens - totalPrice

		logger("[收到请求]: 总价格 %d, 自身价格 %d, 下游价格 %d\n", totalPrice, ownPrice, downstreamPrice)

		if extratoken < 0 {
			logger("[收到请求]: 请求因令牌不足被拒绝。自身价格 %d, 下游价格 %d\n", ownPrice, downstreamPrice)
			return 0, strconv.FormatInt(totalPrice, 10), InsufficientTokens
		}

		if pt.pinpointThroughput {
			pt.Increment() // 增加吞吐量计数
		}

		// 从请求中扣除自身价格，剩余的传给下游
		tokenleft := tokens - ownPrice

		return tokenleft, strconv.FormatInt(totalPrice, 10), nil
	}
	// 如果价格聚合方法不支持，抛出错误
	return 0, "", status.Error(codes.Unimplemented, "不支持的价格聚合方法")
}

// UnaryInterceptorClient 是一个中间服务客户端拦截器的示例。
// 用于服务间调用（Service A -> Service B）。
// [Go概念]: 拦截器 (Interceptor) 类似于中间件，包裹在实际 RPC 调用周围。
func (pt *PriceTable) UnaryInterceptorClient(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// 在发送前：本示例暂未包含发送前的逻辑，主要逻辑在接收响应后
	// 创建 header 变量用于接收服务端返回的元数据
	var header metadata.MD

	// [Go概念]: grpc.Header(&header)
	// 这是一个 CallOption，告诉 gRPC 框架把服务端返回的 Header 写入到我们的 header 变量中。
	// invoker 执行真正的远程调用。
	err := invoker(ctx, method, req, reply, cc, grpc.Header(&header))

	// 收到响应后：更新并存储价格信息供未来使用
	// 服务端会在 Header 中放入 "price" 字段
	if len(header["price"]) > 0 {
		priceDownstream, _ := strconv.ParseInt(header["price"][0], 10, 64)
		// 从上下文中获取当前调用的方法名（需要在发送前设置到 OutgoingContext）
		md, _ := metadata.FromOutgoingContext(ctx)
		methodName := md["method"][0]

		// 更新本地缓存的下游价格 (Market Price Update)
		pt.UpdateDownstreamPrice(ctx, methodName, header["name"][0], priceDownstream)
		logger("[响应后处理]:    收到来自 %s 的价格表\n", header["name"])
	} else {
		logger("[响应后处理]:    未收到价格表\n")
	}

	return err
}

// UnaryInterceptorEnduser 是最终用户（入口）拦截器示例。
// 负责初始令牌的生成、限流检查和注入。
func (pt *PriceTable) UnaryInterceptorEnduser(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

	// [Go概念]: metadata.FromOutgoingContext
	// 获取准备发送出去的元数据
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return errMissingMetadata
	}

	methodName := md["method"][0]
	// 打印元数据中的所有键值对 (调试用)
	if debug {
		logger("[请求前]:    节点 %s 正在调用 %s\n", pt.nodeName, methodName)
		var metadataLog string
		for k, v := range md {
			metadataLog += fmt.Sprintf("%s: %s, ", k, v)
		}
		if metadataLog != "" {
			logger("[终端用户发送请求]: 请求元数据为 %s\n", metadataLog)
		}
	}

	// 如果 `randomRateLimit` > 0，则基于请求ID的最后两位进行随机丢弃
	// 此功能主要用于调试或模拟丢包，生产环境不建议开启。
	if pt.randomRateLimit > 0 {
		// 从元数据获取 request-id
		if requestIDs, found := md["request-id"]; found && len(requestIDs) > 0 {
			reqid, err := strconv.ParseInt(requestIDs[0], 10, 64)
			if err != nil {
				// 解析错误，按需处理
				panic(err)
			}
			// 取 requestID 的最后两位
			lastDigit := reqid % 100
			// 如果最后两位小于阈值，则丢弃请求
			if lastDigit < pt.randomRateLimit {
				logger("[随机丢弃]:  请求被随机丢弃。")
				if pt.fakeInvoker {
					// 伪调用：设置 MaxCallSendMsgSize 为 0 并不实际发送
					opts = append(opts, grpc.MaxCallSendMsgSize(0))
					_ = invoker(ctx, method, req, reply, cc, opts...)
				}
				return status.Error(codes.ResourceExhausted, "客户端被限流，请求被随机丢弃。")
			}
		}
	}

	// [后端概念]: 退避 (Backoff)
	// 如果最近刚被限流过，且还没过冷却时间(clientBackoff)，则直接放弃，不浪费资源去尝试。
	if pt.clientBackoff > 0 && time.Since(pt.lastRateLimitedTime) < pt.clientBackoff {
		if !pt.rateLimitWaiting {
			logger("[触发退避]:    客户端被限流，请求直接丢弃不等待。")
			// 这种情况下直接丢弃，但我们需要通过伪调用来模拟开销并返回错误
			if pt.fakeInvoker {
				// 伪调用开销很大，仅用于测试，默认不开启
				opts = append(opts, grpc.MaxCallSendMsgSize(0))
				_ = invoker(ctx, method, req, reply, cc, opts...)
			}
			return status.Error(codes.ResourceExhausted, "客户端被限流，请求直接丢弃不等待。")
		}
	}

	var tok int64
	// 设置超时计时器，防止客户端等待令牌过久
	startTime := time.Now()

	// 发送前循环：检查价格，计算所需令牌，更新总令牌
	// 这个循环是为了实现“阻塞等待令牌”的逻辑
	for {
		// 如果等待时间超过 ClientTimeOut，返回 RateLimited 错误
		if pt.rateLimiting && pt.rateLimitWaiting && time.Since(startTime) > pt.clientTimeOut {
			logger("[客户端超时]:    等待令牌超时。\n")
			if pt.fakeInvoker {
				opts = append(opts, grpc.MaxCallSendMsgSize(0))
				_ = invoker(ctx, method, req, reply, cc, opts...)
			}
			return status.Errorf(codes.DeadlineExceeded, "等待令牌超时。")
		}

		// 获取当前钱包剩余令牌
		tok = pt.GetTokensLeft()

		// 均匀策略：随机使用 0 到剩余令牌之间的一个值（模拟不同重要性的请求）
		if pt.tokenStrategy == "uniform" {
			if tok > 0 {
				tok = fastrand.Int63n(tok)
			}
		}

		// 如果未开启限流，跳出循环直接发送
		if !pt.rateLimiting {
			break
		}

		// 检查令牌是否足够支付当前价格
		ratelimit := pt.RateLimiting(ctx, tok, methodName)

		// 如果开启了退避机制，更新上次限流时间
		if pt.clientBackoff > 0 {
			if ratelimit == RateLimited && time.Since(pt.lastRateLimitedTime) > pt.clientBackoff {
				logger("[开始退避]:  客户端已被限流，开始退避计时。\n")
				pt.lastRateLimitedTime = time.Now()
			}
		}

		if ratelimit == RateLimited {
			// 如果被限流且不等待（非阻塞模式），直接丢弃
			if !pt.rateLimitWaiting {
				logger("[被限流]: 客户端被限流，请求直接丢弃不等待。\n")
				if pt.fakeInvoker {
					opts = append(opts, grpc.MaxCallSendMsgSize(0))
					_ = invoker(ctx, method, req, reply, cc, opts...)
				}
				return status.Error(codes.ResourceExhausted, "客户端被限流，请求直接丢弃不等待。")
			}

			// 阻塞模式：等待限流器通道信号（等待发工资/令牌补充）
			// [Go概念]: <-pt.rateLimiter 会阻塞当前协程，直到 channel 里有数据或者被关闭
			<-pt.rateLimiter
			// 记录已等待时间和剩余超时时间
			logger("[被限流]: 客户端已等待 %d ms, 距离超时还有 %d ms。\n",
				time.Since(startTime).Milliseconds(), (pt.clientTimeOut - time.Since(startTime)).Milliseconds())
		} else {
			// 令牌充足，跳出循环
			break
		}

		// 尝试扣除令牌
		if pt.DeductTokens(tok) {
			logger("[准备请求]:  从客户端扣除 %d 令牌。\n", tok)
			break
		} else {
			// 扣除失败（可能并发导致余额不足）
			logger("[准备请求]:  令牌不足以扣除 %d, 未扣除。\n", tok)
			if !pt.rateLimitWaiting {
				logger("[被限流]: 客户端被限流，请求直接丢弃不等待。\n")
				if pt.fakeInvoker {
					opts = append(opts, grpc.MaxCallSendMsgSize(0))
					_ = invoker(ctx, method, req, reply, cc, opts...)
				}
				return status.Error(codes.ResourceExhausted, "客户端被限流，请求直接丢弃不等待。")
			}
			// 继续等待
			<-pt.rateLimiter
			logger("[被限流]: 客户端已等待 %d ms, 距离超时还有 %d ms。\n",
				time.Since(startTime).Milliseconds(), (pt.clientTimeOut - time.Since(startTime)).Milliseconds())
		}
	}

	// 将持有的令牌数注入到 Outgoing Context 中，传给下游
	// [Go概念]: gRPC Metadata 是 HTTP/2 Header 的抽象，用于传递 key-value 元数据
	tok_string := strconv.FormatInt(tok, 10)
	ctx = metadata.AppendToOutgoingContext(ctx, "tokens", tok_string, "name", pt.nodeName)

	var header metadata.MD
	err := invoker(ctx, method, req, reply, cc, grpc.Header(&header))

	// 收到响应后，解析并更新价格信息
	if len(header["price"]) > 0 {
		priceDownstream, _ := strconv.ParseInt(header["price"][0], 10, 64)
		pt.UpdateDownstreamPrice(ctx, methodName, header["name"][0], priceDownstream)
		logger("[响应后处理]:    收到来自 %s 的价格表\n", header["name"])
	} else {
		logger("[响应后处理]:    未收到价格表\n")
	}

	return err
}

// UnaryInterceptor 是服务端拦截器示例。
// 它负责：检查令牌、更新价格、执行过载处理 (AQM) 并将价格附加到响应中。
// 这段逻辑运行在服务端收到请求、但还没进入 Handler 之前。
func (pt *PriceTable) UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {

	// 获取请求中携带的元数据 (IncomingContext)
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errMissingMetadata
	}

	if debug {
		var metadataLog string
		for k, v := range md {
			metadataLog += fmt.Sprintf("%s: %s, ", k, v)
		}
		if metadataLog != "" {
			logger("[收到请求]: 请求元数据为 %s\n", metadataLog)
		}
	}

	// 过载处理器：执行 AQM，扣除请求中的令牌，更新价格信息
	var tok int64
	var err error

	// 解析令牌：根据不同的价格聚合策略，令牌可能存储在不同字段
	if pt.priceAggregation == "additive" {
		// 累加策略：可能需要查看针对当前节点的特定令牌 (tokens-nodeName)
		if val, ok := md["tokens-"+pt.nodeName]; ok {
			// 校验格式
			if len(val) > 1 {
				return nil, status.Errorf(codes.InvalidArgument, "重复的令牌字段")
			} else if len(val) == 0 {
				return nil, errMissingMetadata
			}
			tok, _ = strconv.ParseInt(val[0], 10, 64)
		} else {
			// 如果没有特定令牌，查看通用 tokens 字段
			logger("[收到请求]: tokens 为 %s\n", md["tokens"])
			if len(md["tokens"]) > 1 {
				return nil, status.Errorf(codes.InvalidArgument, "重复的令牌字段")
			} else if len(md["tokens"]) == 0 {
				return nil, errMissingMetadata
			}
			tok, _ = strconv.ParseInt(md["tokens"][0], 10, 64)
		}
	} else if pt.priceAggregation == "maximal" || pt.priceAggregation == "mean" {
		// 最大值或平均值策略：直接读取通用的 "tokens" 字段
		if val, ok := md["tokens"]; ok {
			if len(val) > 1 {
				return nil, status.Errorf(codes.InvalidArgument, "重复的令牌字段")
			} else if len(val) == 0 {
				return nil, errMissingMetadata
			}
			tok, _ = strconv.ParseInt(val[0], 10, 64)
		}
	}

	// 核心过载处理：
	methodName := md["method"][0]
	tokenleft, price_string, err := pt.LoadShedding(ctx, tok, methodName)

	// 如果由于令牌不足 (InsufficientTokens) 且开启了负载削减
	if err == InsufficientTokens && pt.loadShedding {
		// 随机性地将当前价格发送给客户端（概率 1/pt.priceFreq）
		// 避免过度的限流反馈导致吞吐量骤降
		if tok%pt.priceFreq == 0 {
			logger("[发送错误响应]:    总价格为 %s\n", price_string)
			// 通过 Header 告知客户端当前价格
			header := metadata.Pairs("price", price_string, "name", pt.nodeName)
			grpc.SendHeader(ctx, header)
		}

		return nil, status.Errorf(codes.ResourceExhausted, "%s 请求被 %s 丢弃。请稍后重试。", methodName, pt.nodeName)
	}
	// 如果发生其他错误（非令牌不足）
	if err != nil && err != InsufficientTokens {
		log.Println(err)
		return nil, err
	}

	// 如果是累加策略，需要将剩余令牌（tokenleft）按照 split 逻辑分发给下游
	if pt.priceAggregation == "additive" {
		// SplitTokens 会计算每个下游应得的令牌，并返回 kv 对
		downstreamTokens, _ := pt.SplitTokens(ctx, tokenleft, methodName)
		// 将分配好的令牌追加到 Outgoing Context 中
		ctx = metadata.AppendToOutgoingContext(ctx, downstreamTokens...)
	}

	// 调用实际的业务处理程序 (Handler)
	m, err := handler(ctx, req)

	// 在发送响应前附加价格信息
	// 目前仅传播该 RPC 方法对应的价格，而不是整个价格表
	// 如果没有开启懒响应 (skipPrice)，则按概率附加价格
	if !pt.skipPrice {
		if tok%pt.priceFreq == 0 {
			header := metadata.Pairs("price", price_string, "name", pt.nodeName)
			logger("[准备响应]:    %s 的总价格为 %s\n", methodName, price_string)
			grpc.SendHeader(ctx, header)
		}
	} else {
		logger("[准备响应]:    懒响应已开启，响应中未附加价格。\n")
	}

	if err != nil {
		logger("RPC 失败，错误: %v", err)
	}
	return m, err
}
