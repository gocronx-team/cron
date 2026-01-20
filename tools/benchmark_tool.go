package main

import (
	"fmt"
	"os"
	"os/exec"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("用法: go run tools/benchmark_tool.go [before|after|quick]")
		return
	}

	switch os.Args[1] {
	case "before":
		runBenchmark("before.txt")
	case "after":
		runBenchmark("after.txt")
	case "quick":
		quickTest()
	default:
		fmt.Println("未知命令")
	}
}

func runBenchmark(outputFile string) {
	fmt.Printf("运行基准测试，结果将保存到: benchmark_results/%s\n", outputFile)
	
	os.MkdirAll("benchmark_results", 0755)
	
	cmd := exec.Command("go", "test", "-bench=.", "-benchmem", 
		"-benchtime=500ms", "-count=1", "-run=^$")
	
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("错误: %v\n输出: %s\n", err, string(output))
		return
	}
	
	os.WriteFile("benchmark_results/"+outputFile, output, 0644)
	fmt.Println(string(output))
	fmt.Println("✓ 测试完成")
}

func quickTest() {
	fmt.Println("运行快速测试...")
	cmd := exec.Command("go", "test", "-bench=.", "-benchmem", 
		"-benchtime=200ms", "-count=1", "-run=^$")
	
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("错误: %v\n", err)
	}
	fmt.Println(string(output))
}
