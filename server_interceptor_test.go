// server_test.go
package rajomon

import (
	"context"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	// defaultOpts 定义了测试用的默认配置
	// 这些参数模拟了 Rajomon 系统的典型运行环境
	defaultOpts = map[string]interface{}{
		"priceUpdateRate":  5000 * time.Microsecond,   // 价格更新频率
		"tokenUpdateRate":  100000 * time.Microsecond, // 令牌更新频率
		"latencyThreshold": 500 * time.Microsecond,    // 延迟阈值
		"priceStep":        int64(180),                // 价格调整步长
		"priceStrategy":    "expdecay",                // 价格策略：指数衰减
		"lazyResponse":     false,                     // 是否懒响应 (是否每次都返回价格)
		"rateLimiting":     true,                      // 开启限流 (客户端侧)
		"loadShedding":     true,                      // 开启负载削减 (服务端侧)
	}

	// defaultCallMap 定义了服务间的调用关系（此处为空，表示没有下游依赖）
	// 在复杂的微服务测试中，这里会定义 Service A -> Service B 的依赖关系
	defaultCallMap = map[string][]string{
		"Foo": {}, // Foo 方法没有下游依赖 (no downstreams)
	}
)

// helper to make a fresh PriceTable per test
// 辅助函数：为每个测试创建一个全新的 Rajomon PriceTable 实例
// 这保证了测试之间的隔离性，防止前一个测试修改了状态影响下一个测试
func newTestRajomon() *PriceTable {
	return NewRajomon("node-1", defaultCallMap, defaultOpts)
}

// TestUnaryInterceptor_RejectsLowTokens 测试当 Token 不足时，拦截器是否能正确拒绝请求
// 这是一个典型的“反向测试用例” (Negative Test Case)
func TestUnaryInterceptor_RejectsLowTokens(t *testing.T) {
	// 1. 准备环境 (Arrange)
	// 完全按照指定配置构建实例
	pt := newTestRajomon()

	// 手动设置价格表：自身价格=100
	// 注意：Rajomon 通常取所有相关价格的最大值作为门槛
	pt.priceTableMap.Store("ownprice", int64(100))
	// pt.priceTableMap.Store("Foo", int64(50))

	// 2. 模拟请求上下文 (Mock Context)
	// 创建上下文，模拟客户端在 Header 中携带 tokens=10
	// 预期：10 < 100，钱不够，应该被拒绝
	md := metadata.Pairs(
		"tokens", "10",
		"method", "Foo", // 指定调用的方法名
	)
	// NewIncomingContext 模拟了服务端接收到的上下文
	ctx := metadata.NewIncomingContext(context.Background(), md)

	// 3. 定义伪处理程序 (Stub Handler)
	// 这个函数模拟了真正的业务逻辑。
	// 如果拦截器工作正常，它会拦截请求，这个函数永远不应该被调用。
	called := false
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		called = true
		return "SHOULD_NOT_REACH", nil
	}

	// 4. 执行测试 (Act)
	// 直接调用 UnaryInterceptor 函数，就像 gRPC 框架做的那样
	resp, err := pt.UnaryInterceptor(
		ctx,
		nil, // request (本次测试不关心具体请求内容)
		&grpc.UnaryServerInfo{FullMethod: "Foo"},
		handler,
	)

	// 5. 验证结果 (Assert)
	// 验证结果：应该报错
	if err == nil {
		t.Fatalf("预期应返回错误，但得到了响应: resp=%v", resp)
	}
	// 验证错误码：应该是 ResourceExhausted (资源耗尽/过载)，这是 gRPC 标准限流错误码
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("预期错误码 ResourceExhausted; 实际得到 %v", err)
	}
	// 验证 Handler 是否被执行 (确保拦截器没有“漏网”)
	if called {
		t.Error("尽管令牌不足，handler 依然被调用了")
	}
}

// TestUnaryInterceptor_AllowsHighTokens 测试当 Token 充足时，拦截器是否允许通过
// 这是一个“正向测试用例” (Positive Test Case)
func TestUnaryInterceptor_AllowsHighTokens(t *testing.T) {

	pt := newTestRajomon()

	// 自身价格=10，Token=20，足以支付
	pt.priceTableMap.Store("ownprice", int64(10))
	// pt.priceTableMap.Store("Foo", int64(5))

	// tokens=20 >= price(10,5)->max=10
	md := metadata.Pairs(
		"tokens", "20",
		"method", "Foo",
	)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	// 伪处理程序，应该被调用并返回 "OKAY"
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "OKAY", nil
	}

	resp, err := pt.UnaryInterceptor(
		ctx,
		nil,
		&grpc.UnaryServerInfo{FullMethod: "Foo"},
		handler,
	)

	// 验证结果：不应报错
	if err != nil {
		t.Fatalf("预期无错误; 实际得到 %v", err)
	}
	// 验证响应内容是否正确透传
	if resp != "OKAY" {
		t.Errorf("非预期的响应: %v; 期望 %q", resp, "OKAY")
	}
}

// TestUnaryInterceptor_MixedTokens 测试混合流量（一部分Token不足，一部分充足）的情况
// 模拟真实生产环境中的动态流量
func TestUnaryInterceptor_MixedTokens(t *testing.T) {
	pt := newTestRajomon()
	pt.priceTableMap.Store("ownprice", int64(10))
	// pt.priceTableMap.Store("Foo", int64(2))

	var rejected, accepted int
	// 循环 10 次模拟请求
	for i := 0; i < 10; i++ {
		tokenVal := 3
		// 前 5 次 Token=3 (不足，应拒绝)，后 5 次 Token=20 (充足，应通过)
		if i >= 5 {
			tokenVal = 20
		}
		// strconv.Itoa 用于将整数转换为字符串
		md := metadata.Pairs("tokens", strconv.Itoa(tokenVal), "method", "Foo")
		ctx := metadata.NewIncomingContext(context.Background(), md)

		called := false
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			called = true
			return "OK", nil
		}

		resp, err := pt.UnaryInterceptor(
			ctx, nil, &grpc.UnaryServerInfo{FullMethod: "Foo"}, handler,
		)

		// 验证前 5 次拒绝逻辑
		if i < 5 {
			if status.Code(err) != codes.ResourceExhausted {
				t.Errorf("第 %d 次迭代: 预期拒绝; 实际 resp=%v, err=%v", i, resp, err)
			}
			rejected++
		} else {
			// 验证后 5 次通过逻辑
			if err != nil {
				t.Errorf("第 %d 次迭代: 预期通过; 实际 err=%v", i, err)
			}
			if resp != "OK" {
				t.Errorf("第 %d 次迭代: 非预期的 resp=%v; 期望 %q", i, resp, "OK")
			}
			if !called {
				t.Errorf("第 %d 次迭代: 高 Token 请求未触发 handler 调用", i)
			}
			accepted++
		}
	}

	// 最终统计验证
	if rejected != 5 || accepted != 5 {
		t.Errorf("混合 Token 测试结果: 拒绝数=%d, 通过数=%d; 期望各 5 个", rejected, accepted)
	}
}

// TestDownstreamPrice_StorageAndRetrieval 测试下游服务价格的存储与检索逻辑
// 验证 Rajomon 如何处理依赖服务的价格变化
func TestDownstreamPrice_StorageAndRetrieval(t *testing.T) {
	// 定义调用关系：Bar 服务依赖下游 X 和 Y
	// 这模拟了复杂的微服务链路
	callMap := map[string][]string{
		"Bar": {"X", "Y"},
	}
	pt := NewRajomon("node-2", callMap, defaultOpts)

	// 1. 初始状态检查
	// 下游价格应为 0 (初始值)
	downstreamPrice, err := pt.RetrieveDSPrice(context.Background(), "Bar")
	if downstreamPrice != 0 {
		t.Errorf("预期初始下游价格为 0; 实际得到 %d", downstreamPrice)
	}
	if err != nil {
		t.Fatalf("RetrieveDSPrice 错误: %v", err)
	}

	// 2. 更新单一下游价格
	// 假设收到了下游 X 的响应，价格为 15
	newPriceX := int64(15)
	price, err := pt.UpdateDownstreamPrice(context.Background(), "Bar", "X", newPriceX)
	if err != nil {
		t.Fatalf("UpdateDownstreamPrice 错误: %v", err)
	}
	if price != newPriceX {
		t.Errorf("返回的价格 = %d; 期望 %d", price, newPriceX)
	}

	// 再次检索，Bar 方法的下游价格应更新为 15
	got, err := pt.RetrieveDSPrice(context.Background(), "Bar")
	if err != nil {
		t.Fatalf("RetrieveDSPrice 错误: %v", err)
	}
	if got != newPriceX {
		t.Errorf("RetrieveDSPrice = %d; 期望 %d", got, newPriceX)
	}

	// 3. 多下游价格聚合测试 (Maximal Strategy)
	// 更新下游服务 Y 的价格为 5 (小于当前的 15)
	// Rajomon 默认采用 "maximal" (最大值) 聚合策略
	// 因为并发调用时，整个链路的延迟取决于最慢的那个，所以价格也取决于最高的那个
	_, _ = pt.UpdateDownstreamPrice(context.Background(), "Bar", "Y", int64(5))
	got2, err := pt.RetrieveDSPrice(context.Background(), "Bar")
	if got2 != newPriceX {
		t.Errorf("较低价格更新后, DSPrice = %d; 期望 %d (取最大值)", got2, newPriceX)
	}
	if err != nil {
		t.Fatalf("RetrieveDSPrice 错误: %v", err)
	}

	// 更新下游服务 Y 的价格为 20 (高于当前的 15)
	// 总价格应更新为新的最大值 20
	_, _ = pt.UpdateDownstreamPrice(context.Background(), "Bar", "Y", int64(20))
	got3, err := pt.RetrieveDSPrice(context.Background(), "Bar")
	if got3 != int64(20) {
		t.Errorf("较高价格更新后, DSPrice = %d; 期望 %d (取最大值)", got3, int64(20))
	}
	if err != nil {
		t.Fatalf("RetrieveDSPrice 错误: %v", err)
	}
}

// TestLoadShedding_ReturnsCorrectPrice 验证 LoadShedding 函数是否返回正确的价格字符串和剩余 Token
// 验证扣费逻辑
func TestLoadShedding_ReturnsCorrectPrice(t *testing.T) {
	pt := newTestRajomon()
	pt.priceTableMap.Store("ownprice", int64(7))

	// 输入 Token=20，价格=7
	tokens, price, err := pt.LoadShedding(context.Background(), 20, "Foo")
	if err != nil {
		t.Fatalf("非预期的错误: %v", err)
	}
	// 验证返回的价格字符串 (用于 Header 传回给客户端)
	if price != "7" {
		t.Errorf("price = %q; 期望 '7'", price)
	}
	// 验证剩余 Token：20 - 7 = 13
	// tokens left = 20 - price(7)
	if tokens != 13 {
		t.Errorf("剩余 tokens = %d; 期望 13", tokens)
	}
}
