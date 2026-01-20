// server_concurrency_test.go
package rajomon

import (
	"context"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	// [Go 注意]: 这里需要引入实际的 Protobuf 生成代码
	// 请替换为你项目中生成的 .pb.go 文件的包路径
	pb "github.com/Jiali-Xing/protobuf"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// busyLoop 是一个忙等待循环，用于模拟 CPU 占用
// 实际上此测试中的 computation 主要依赖 time.After 进行阻塞
// 但 busyLoop 结构保留了消耗 CPU 周期的能力
func busyLoop(c chan<- int, quit chan bool) {
	for {
		if <-quit {
			return
		}
	}
}

// computation 模拟耗时的计算任务
// duration: 模拟耗时的时长（毫秒）
// 这个函数的作用是让每个请求都“卡”一会，从而在并发量大时制造排队积压
func computation(duration int) {
	// Jiali: 下面的代码块实现了伪计算负载
	quit := make(chan bool)
	busyChan := make(chan int)
	go busyLoop(busyChan, quit)

	select {
	case busyResult := <-busyChan:
		log.Println(busyResult)
	// 在这里阻塞指定的时间，模拟处理延迟
	case <-time.After(time.Duration(duration) * time.Millisecond):
		// log.Println("timed out")
	}
	quit <- true
	return
}

// greetingServer 是一个应用了 Rajomon AQM (主动队列管理) 的最小化服务端实现
// 它实现了 pb.GreetingServiceServer 接口
type greetingServer struct {
	// 嵌入 UnimplementedGreetingServiceServer 以满足接口兼容性
	pb.UnimplementedGreetingServiceServer
	// 持有 Rajomon 的 PriceTable 引用，虽然拦截器才是主要工作者，但 Server 可能需要读取状态
	pt *PriceTable
}

// Greeting 是 gRPC 的处理函数 (Handler)
func (s *greetingServer) Greeting(ctx context.Context, req *pb.GreetingRequest) (*pb.GreetingResponse, error) {
	// [模拟负载]: 每个请求处理耗时 5 秒
	// 在 1000 并发下，这会迅速耗尽 Go 调度器的资源，导致 Goroutine 排队延迟飙升
	// Rajomon 的 queuingCheck 协程应该能探测到这个延迟
	computation(5000)

	// 获取传入的问候信息
	incoming := req.GetGreeting()

	// 创建一个新的服务端问候信息
	serverGreet := &pb.Greeting{
		Service: "server", // 或者使用 s.name
		// 在这里填充其他字段
	}

	// 将它们组合到响应切片中
	resp := &pb.GreetingResponse{
		Greeting: []*pb.Greeting{
			incoming,
			serverGreet,
		},
	}
	return resp, nil
}

var (
	// Opts 定义了 Rajomon 的配置参数，专门针对此测试调整
	Opts = map[string]interface{}{
		"priceUpdateRate":  5000 * time.Microsecond,   // 价格更新频率：5ms (很频繁，为了快速反应)
		"tokenUpdateRate":  100000 * time.Microsecond, // 令牌更新频率：100ms
		"latencyThreshold": 500 * time.Microsecond,    // 延迟阈值：0.5ms (只要排队超过 0.5ms 就认为拥塞)
		"priceStep":        int64(180),                // 价格每次上涨的步长 (激进涨价)
		"priceStrategy":    "expdecay",                // 价格策略：指数衰减 (上涨快，下降慢)
		"lazyResponse":     false,                     // 关闭懒响应 (强制每次携带价格，方便测试观测)
		"rateLimiting":     true,                      // 开启限流
		"loadShedding":     true,                      // 开启负载削减 (Load Shedding)
		"pinpointQueuing":  true,                      // 开启基于排队延迟的检测 (这是本测试的核心验证点)
		// "debug":            true,
	}
)

// TestHighConcurrencyPriceIncrease 测试在高并发负载下，价格是否会正确上涨
// 验证 Rajomon 是否具备“拥塞感知”和“自适应定价”能力
func TestHighConcurrencyPriceIncrease(t *testing.T) {
	// 1) 准备 Rajomon 实例
	// 定义调用关系：Greeting 服务没有下游依赖
	callMap := map[string][]string{"Greeting": {}}
	pt := NewRajomon("node-1", callMap, Opts)

	// 2) 在端口 :50051 启动 gRPC 服务器
	// net.Listen 监听 TCP 端口
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}

	// 注册 Rajomon 的拦截器 (UnaryInterceptor)
	// [关键点]: grpc.NewServer 时传入 Interceptor，这样所有请求都会先经过 Rajomon 的逻辑
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(pt.UnaryInterceptor),
	)
	pb.RegisterGreetingServiceServer(grpcServer, &greetingServer{pt: pt})

	// 在后台 goroutine 中启动服务，避免阻塞测试主线程
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("grpc 服务启动错误: %v", err)
		}
	}()
	// 测试结束后自动停止服务器，释放端口
	defer grpcServer.Stop()

	// 让服务器预热一会，确保端口已监听
	time.Sleep(1 * time.Second)

	// 4) 准备压测参数
	const concurrency = 1000              // 并发数：1000 个客户端协程
	const testDuration = 10 * time.Second // 测试持续时间

	stop := make(chan struct{})

	// 3) 建立连接并发射 RPC 请求
	// 使用 WaitGroup 等待所有客户端协程结束
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// 建立非加密连接 (Insecure) 简化测试
			conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
			if err != nil {
				t.Errorf("连接失败: %v", err)
				return
			}
			defer conn.Close()

			client := pb.NewGreetingServiceClient(conn)

			// 注入元数据
			// [关键点]: 客户端需要携带 tokens。这里给 1000，虽然初始价格是 0，
			// 但随着价格上涨，1000 可能最终也不够用，从而触发 Load Shedding。
			ctx := metadataAppend(context.Background(), "method", "Greeting", "tokens", "1000")

			// 每个客户端无限循环发送请求，直到收到停止信号
			for {
				select {
				case <-stop:
					return
				default:
					// 发起 RPC 调用
					resp, err := client.Greeting(ctx, &pb.GreetingRequest{Greeting: &pb.Greeting{Service: "client"}})
					if err != nil {
						// [验证逻辑]: 我们允许 ResourceExhausted 错误。
						// 因为当 Rajomon 检测到过载（排队严重）时，会主动拒绝请求。
						// 如果出现了其他错误（如网络断开），则测试失败。
						if status.Code(err) != codes.ResourceExhausted {
							t.Errorf("未预期的错误: %v", err)
						}
					} else {
						// 如果请求成功，响应不应为空
						if resp == nil {
							t.Errorf("Greeting 响应为空")
						}
					}
				}
			}
		}()
	}

	// 运行测试时间的一半，让负载持续一段时间
	time.Sleep(testDuration / 2)

	// (调试用) 打印价格表内容 (注释状态)
	// pt.priceTableMap.Range(...)

	// 5) 验证价格是否大于 0
	// 在 1000 并发和 5秒处理延迟的压力下，队列应该已经爆炸。
	// 如果 Rajomon 工作正常，它会检测到高 queuing delay 并调高价格。
	priceStr, err := pt.RetrieveTotalPrice(context.Background(), "Greeting")
	if err != nil {
		t.Fatalf("RetrieveTotalPrice 获取总价错误: %v", err)
	}

	if priceStr == "0" {
		t.Errorf("在负载下价格未上涨，仍为 %s", priceStr)
	} else {
		t.Logf("负载后观察到的价格: %s", priceStr)
	}

	// 4) 继续运行剩余时间，观察系统行为
	time.Sleep(testDuration / 2)

	// 停止所有客户端
	close(stop)
	wg.Wait()
}

// metadataAppend 是一个小助手函数，用于将键值对添加到上下文的元数据中
// 方便构造 gRPC 的 Outgoing Context
func metadataAppend(ctx context.Context, kv ...string) context.Context {
	md := metadata.Pairs(kv...)
	return metadata.NewOutgoingContext(ctx, md)
}
